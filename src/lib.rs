use std::collections::HashMap;
use std::fs::File; 
use serde::{Serialize, Deserialize};
use bincode::{serialize_into, deserialize_from};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    id: u64,
    data: HashMap<String, String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Table {
    name: String,
    records: Vec<Record>,
    index: HashMap<u64, usize>,
}

#[derive(Serialize, Deserialize)]
pub struct Database {
    tables: HashMap<String, Table>,
}

enum SqlStatement {
    Select {
        table: String,
        columns: Vec<String>,
        condition: Option<Condition>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<String>,
    },
    Update {
        table: String,
        column: String,
        value: String,
        condition: Option<Condition>,
    },
    Delete {
        table: String,
        condition: Option<Condition>,
    },
}

#[derive(Clone)]
enum Condition {
    Equals(String, String),
    NotEquals(String, String),
    GreaterThan(String, String),
    LessThan(String, String),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}


impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: String) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            Err(format!("Table '{}' already exists", name))
        } else {
            let table = Table {
                name: name.clone(),
                records: Vec::new(),
                index: HashMap::new(),
            };
            self.tables.insert(name, table);
            Ok(())
        }
    }

    pub fn insert(&mut self, table_name: &str, id: u64, data: HashMap<String, String>) -> Result<(), String> {
        if let Some(table) = self.tables.get_mut(table_name) {
            if table.index.contains_key(&id) {
                Err(format!("Record with id {} already exists in table '{}'", id, table_name))
            } else {
                let record = Record { id, data };
                let index = table.records.len();
                table.records.push(record);
                table.index.insert(id, index);
                Ok(())
            }
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    pub fn get(&self, table_name: &str, id: u64) -> Result<Option<&Record>, String> {
        if let Some(table) = self.tables.get(table_name) {
            Ok(table.index.get(&id).map(|&index| &table.records[index]))
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    pub fn get_all(&self, table_name: &str) -> Result<Vec<&Record>, String> {
        if let Some(table) = self.tables.get(table_name) {
            Ok(table.records.iter().collect())
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    pub fn update(&mut self, table_name: &str, id: u64, data: HashMap<String, String>) -> Result<(), String> {
        if let Some(table) = self.tables.get_mut(table_name) {
            if let Some(&index) = table.index.get(&id) {
                table.records[index].data = data;
                Ok(())
            } else {
                Err(format!("Record with id {} not found in table '{}'", id, table_name))
            }
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    pub fn delete(&mut self, table_name: &str, id: u64) -> Result<(), String> {
        if let Some(table) = self.tables.get_mut(table_name) {
            if let Some(index) = table.index.remove(&id) {
                table.records.remove(index);
                // Update indices for all records after the deleted one
                for (_, idx) in table.index.iter_mut() {
                    if *idx > index {
                        *idx -= 1;
                    }
                }
                Ok(())
            } else {
                Err(format!("Record with id {} not found in table '{}'", id, table_name))
            }
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(AsRef::as_ref).collect()
    }

    pub fn query(&self, table_name: &str, condition: impl Fn(&Record) -> bool) -> Result<Vec<&Record>, String> {
        if let Some(table) = self.tables.get(table_name) {
            Ok(table.records.iter().filter(|r| condition(r)).collect())
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    
    pub fn execute_sql(&mut self, sql: &str) -> Result<Vec<Record>, String> {
        let statement = self.parse_sql(sql)?;
        match statement {
            SqlStatement::Select { table, columns, condition } => self.execute_select(&table, &columns, condition),
            SqlStatement::Insert { table, columns, values } => self.execute_insert(&table, &columns, &values),
            SqlStatement::Update { table, column, value, condition } => self.execute_update(&table, &column, &value, condition),
            SqlStatement::Delete { table, condition } => self.execute_delete(&table, condition),
        }
    }

    fn parse_sql(&self, sql: &str) -> Result<SqlStatement, String> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        match tokens[0].to_uppercase().as_str() {
            "SELECT" => {
                let from_index = tokens.iter().position(|&r| r.to_uppercase() == "FROM").ok_or("Invalid SELECT statement")?;
                let table = tokens[from_index + 1].to_string();
                let columns = tokens[1..from_index].iter().map(|s| s.to_string()).collect();
                let condition = self.parse_where_clause(&tokens[from_index + 2..]);
                Ok(SqlStatement::Select { table, columns, condition })
            },
            "INSERT" => { 
                let into_index = tokens.iter().position(|&r| r.to_uppercase() == "INTO").ok_or("Invalid INSERT statement")?;
                let values_index = tokens.iter().position(|&r| r.to_uppercase() == "VALUES").ok_or("Invalid INSERT statement")?;
                let table = tokens[into_index + 1].to_string();
                let columns = tokens[into_index + 2..values_index].iter()
                    .map(|s| s.trim_matches(|c| c == '(' || c == ',' || c == ')').to_string())
                    .collect();
                let values = tokens[values_index + 1..].iter()
                    .map(|s| s.trim_matches(|c| c == '(' || c == ',' || c == ')').to_string())
                    .collect();
                Ok(SqlStatement::Insert { table, columns, values })
            },
            "UPDATE" => {
                let set_index = tokens.iter().position(|&r| r.to_uppercase() == "SET").ok_or("Invalid UPDATE statement")?;
                let table = tokens[1].to_string();
                let column = tokens[set_index + 1].to_string();
                let value = tokens[set_index + 3].to_string();
                let condition = self.parse_where_clause(&tokens[set_index + 4..]);
                Ok(SqlStatement::Update { table, column, value, condition })
            },
            "DELETE" => {
                let from_index = tokens.iter().position(|&r| r.to_uppercase() == "FROM").ok_or("Invalid DELETE statement")?;
                let table = tokens[from_index + 1].to_string();
                let condition = self.parse_where_clause(&tokens[from_index + 2..]);
                Ok(SqlStatement::Delete { table, condition })
            },
            _ => Err("Unsupported SQL statement".to_string()),
        }
    }

    fn parse_where_clause(&self, tokens: &[&str]) -> Option<Condition> {
        if tokens.is_empty() || tokens[0].to_uppercase() != "WHERE" {
            return None;
        }

        let mut conditions = Vec::new();
        let mut i = 1;
        while i < tokens.len() {
            if i + 2 < tokens.len() {
                let column = tokens[i].to_string();
                let operator = tokens[i + 1];
                let value = tokens[i + 2].to_string();
                let condition = match operator {
                    "=" => Condition::Equals(column, value),
                    "!=" => Condition::NotEquals(column, value),
                    ">" => Condition::GreaterThan(column, value),
                    "<" => Condition::LessThan(column, value),
                    _ => return None, // Unsupported 
                };
                conditions.push(condition);
                i += 3;
            } else {
                break;
            }

            if i < tokens.len() {
                match tokens[i].to_uppercase().as_str() {
                    "AND" => i += 1,
                    "OR" => {
                        let left = conditions.pop().unwrap();
                        let right = self.parse_where_clause(&tokens[i + 1..]).unwrap();
                        conditions.push(Condition::Or(Box::new(left), Box::new(right)));
                        break;
                    },
                    _ => break,
                }
            }
        }

        conditions.into_iter().reduce(|acc, item| Condition::And(Box::new(acc), Box::new(item)))
    }

    fn execute_select(&self, table: &str, columns: &[String], condition: Option<Condition>) -> Result<Vec<Record>, String> {
        let table = self.tables.get(table).ok_or("Table not found")?;
        let records: Vec<Record> = table.records.iter()
            .filter(|record| self.evaluate_condition(record, &condition))
            .cloned()
            .collect();

        if columns[0] == "*" {
            Ok(records)
        } else {
            Ok(records.into_iter()
                .map(|mut record| {
                    record.data.retain(|k, _| columns.contains(k));
                    record
                })
                .collect())
        }
    }

    fn execute_insert(&mut self, table: &str, columns: &[String], values: &[String]) -> Result<Vec<Record>, String> {
        let table = self.tables.get_mut(table).ok_or("Table not found")?;
        let id = table.records.len() as u64 + 1; 
        let mut data = HashMap::new();
        for (column, value) in columns.iter().zip(values.iter()) {
            data.insert(column.clone(), value.clone());
        }
        let record = Record { id, data };
        table.records.push(record.clone());
        table.index.insert(id, table.records.len() - 1);
        Ok(vec![record])
    }
 
    fn execute_delete(&mut self, table: &str, condition: Option<Condition>) -> Result<Vec<Record>, String> {
        // 1. evaluate the condition and collect the IDs to delete
        let ids_to_delete = {
            let table = self.tables.get(table).ok_or("Table not found")?;
            table.records.iter()
                .filter(|record| self.evaluate_condition(record, &condition))
                .map(|record| record.id)
                .collect::<Vec<_>>()
        };
    
        // 2. perform the deletion
        let table = self.tables.get_mut(table).ok_or("Table not found")?;
        let mut deleted_records = Vec::new();
    
        for id in ids_to_delete {
            if let Some(index) = table.index.remove(&id) {
                let record = table.records.remove(index);
                deleted_records.push(record);
                // Update indices for all records after the deleted one
                for (_, idx) in table.index.iter_mut() {
                    if *idx > index {
                        *idx -= 1;
                    }
                }
            }
        }
    
        Ok(deleted_records)
    }
    fn execute_update(&mut self, table: &str, column: &str, value: &str, condition: Option<Condition>) -> Result<Vec<Record>, String> {
        // 1. evaluate the condition and collect the IDs to update
        let ids_to_update = {
            let table = self.tables.get(table).ok_or("Table not found")?;
            table.records.iter()
                .filter(|record| self.evaluate_condition(record, &condition))
                .map(|record| record.id)
                .collect::<Vec<_>>()
        };
    
        // 2. perform the update
        let table = self.tables.get_mut(table).ok_or("Table not found")?;
        let mut updated_records = Vec::new();
    
        for id in ids_to_update {
            if let Some(index) = table.index.get(&id) {
                if let Some(data) = table.records[*index].data.get_mut(column) {
                    *data = value.to_string();
                    updated_records.push(table.records[*index].clone());
                }
            }
        }
    
        Ok(updated_records)
    }
    fn evaluate_condition(&self, record: &Record, condition: &Option<Condition>) -> bool {
        match condition {
            Some(cond) => match cond {
                Condition::Equals(col, val) => record.data.get(col).map_or(false, |v| v == val),
                Condition::NotEquals(col, val) => record.data.get(col).map_or(true, |v| v != val),
                Condition::GreaterThan(col, val) => record.data.get(col).map_or(false, |v| v > val),
                Condition::LessThan(col, val) => record.data.get(col).map_or(false, |v| v < val),
                Condition::And(left, right) => self.evaluate_condition(record, &Some(*left.clone())) && self.evaluate_condition(record, &Some(*right.clone())),
                Condition::Or(left, right) => self.evaluate_condition(record, &Some(*left.clone())) || self.evaluate_condition(record, &Some(*right.clone())),
            },
            None => true,
        }
    }

    pub fn save(&self, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(filename)?;
        serialize_into(file, self)?;
        Ok(())
    }

    pub fn load(filename: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(filename)?;
        let db: Database = deserialize_from(file)?;
        Ok(db)
    }
}
