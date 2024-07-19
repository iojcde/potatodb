use potatodb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::new();
     
    db.create_table("users".to_string())?;
  
    db.execute_sql("INSERT INTO users (name, age, email) VALUES (Alice, 30, alice@example.com)")?;
    db.execute_sql("INSERT INTO users (name, age, email) VALUES (Bob, 25, bob@example.com)")?;
    db.execute_sql("INSERT INTO users (name, age, email) VALUES (Charlie, 35, charlie@example.com)")?;
    db.execute_sql("INSERT INTO users (name, age, email) VALUES ('Diana', 28, 'diana@example.com')")?;
    db.execute_sql("INSERT INTO users (name, age, email) VALUES ('Evan', 40, 'evan@example.com')")?;
    db.execute_sql("UPDATE users SET age = 29 WHERE name = 'Diana'")?;
    db.execute_sql("DELETE FROM users WHERE name = 'Bob'")?;

    let tables = db.list_tables();
    println!("Tables: {:?}", tables);

 
    db.save("database.bin")?;
    println!("Database saved successfully");
 
    let loaded_db = Database::load("database.bin")?;
    println!("Database loaded successfully");
     
    let tables = loaded_db.list_tables();
    println!("Loaded tables: {:?}", tables);

    let select_result = db.execute_sql("SELECT * FROM users")?;
    println!("Select result: {:?}", select_result);

    Ok(())
}