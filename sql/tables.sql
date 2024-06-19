CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(256) NOT NULL,
    password VARCHAR(256) NOT NULL,
    UNIQUE (username)
);

CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_id ON users(id);

CREATE TABLE downloads (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    link VARCHAR(4096) NOT NULL,
    file_name VARCHAR(256) NOT NULL,
    completed BOOLEAN DEFAULT FALSE,
    error VARCHAR DEFAULT '',
    UNIQUE (user_id, link),
    CONSTRAINT fk_user
        FOREIGN KEY(user_id) 
        REFERENCES users(id)
);

CREATE INDEX idx_downloads_id ON downloads(id);
CREATE INDEX idx_downloads_user_id ON downloads(user_id);
