## Run
1. Create a docker network
    ```shell
    docker network create kafka-connection
    ```

2. Start Kafka infrastructure
    ```shell
    cd kafka
    docker compose up -d
    ```

3. Start producer/consumer/database
    ```shell
    cd consumer
    docker compose up --build -d
    ```

4. Check databse
    ```shell
    # in host shell
    docker exec -it postgres bash
    ```
    ```shell
    # in container shell
    # to run postgres
    psql -U postgres
    ```
    ```shell
    # in postgres cli
    # use database kafka
    \c kafka
    # show tables
    \d
    # look at content of topics
    SELECT * FROM data;

    # look at headers
    SELECT * FROM headers;
    ```