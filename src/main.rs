extern crate serde;
extern crate serde_json;
extern crate rusqlite;
#[macro_use]
extern crate serde_derive;

use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::path::Path;
use rusqlite::Connection;
use rusqlite::OpenFlags;
use rusqlite::NO_PARAMS;
use rusqlite::types::ToSql;


#[derive(Deserialize)]
pub struct Comment {
    parent_id: String,
    created_utc: String,
    subreddit_id: String,
    subreddit: String,
    id: String,
    author: String,
    score: i16,
    link_id: String,
    body: String,
    name: String,
}

fn main() {
    let mut file = File::open("/home/n41r0j/Downloads/RC_2012-12").unwrap();
    let mut buf_reader = BufReader::new(file);

    let flag = OpenFlags::default();

    let conn = Connection::open_with_flags(Path::new("/home/n41r0j/rust-reddit3.db"), flag).unwrap();
//    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE IF NOT EXISTS subs(subreddit_id VARCHAR(255),subreddit VARCHAR(255),PRIMARY KEY(subreddit_id));", NO_PARAMS).unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS users(id VARCHAR(255) PRIMARY KEY,author VARCHAR(255));", NO_PARAMS).unwrap();

    conn.execute("CREATE TABLE IF NOT EXISTS posts(
parent_id VARCHAR(255) PRIMARY KEY,
score INTEGER,
created_utc INTEGER,
link_id VARCHAR(255),
body VARCHAR(8000),
name VARCHAR(255),
author VARCHAR(255),
subreddit VARCHAR(255),
FOREIGN KEY (author) REFERENCES users(author),
FOREIGN KEY (subreddit) REFERENCES subs(subreddit)
);",
                 NO_PARAMS).unwrap();

//    conn.execute_batch("PRAGMA journal_mode=WAL");
    conn.execute_batch("PRAGMA synchronous=OFF");

//    conn.query_row("PRAGMA journal_mode=WAL;", NO_PARAMS, |_| { });

    for (num, line) in buf_reader.lines().enumerate() {
        let comment: Comment = serde_json::from_str(&line.unwrap()).unwrap();

        conn.execute(
            "INSERT OR IGNORE INTO subs(subreddit_id, subreddit) VALUES(?1, ?2)",
            &[&comment.subreddit_id, &comment.subreddit]
        ).unwrap();

        conn.execute(
            "INSERT OR IGNORE INTO users(id, author) VALUES(?1, ?2)",
            &[&comment.id, &comment.author]
        ).unwrap();

        conn.execute(
            "INSERT OR IGNORE INTO posts(parent_id, score, created_utc, link_id, body, name, author, subreddit) VALUES(?,?,?,?,?,?,?,?)",
            &[
                &comment.parent_id,
                &comment.score as &ToSql,
                &comment.created_utc,
                &comment.link_id,
                &comment.body,
                &comment.name,
                &comment.author,
                &comment.subreddit]
        ).unwrap();
    }
}

