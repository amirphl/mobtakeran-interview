## Run
- Execute `docker compose up -d`
- Download go packages (`go mod download`)
- run `docker exec -it interview-postgres-1 bash -c "psql -U postgres downloadslinks"`
    - Now copy content of the `tables.sql` and paste into the terminal to create the necessary tables.
- Now execute `bash run.sh`

## Usage
- register user 
    - `curl 127.0.0.1:8080/register -X POST -d '{"username": "amiramir", "password": "mypassword"}'`
- login user
    - `curl 127.0.0.1:8080/login -X POST -d '{"username": "amiramir", "password": "mypassword"}'`
    - sample response: `{"token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MTkwODQxMjUsInVzZXJfaWQiOjN9.BwgyLJqhUfw1J-9No_2e0Lrnso7ynxqRdckS_kXGPLQ"}`
- download link
    - `curl 127.0.0.1:8080/downloads/ -X POST -d '{"link": "https://news-cdn.varzesh3.com/pictures/2024/06/19/D/htx5e4yd.jpg?w=800"}' -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MTkwODQxMjUsInVzZXJfaWQiOjN9.BwgyLJqhUfw1J-9No_2e0Lrnso7ynxqRdckS_kXGPLQ'`

## TODO
- proper logging
- connection pooling for Redis and Postgres and HTTP clients
- write tests
- proper naming of downloaded files (extension), make sure there is enough space
- *** scheduler for rerunning uncompleted jobs
- manual action for errors while downloading the files
