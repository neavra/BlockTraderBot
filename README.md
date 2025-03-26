# BlockTraderBot
A bot that is used to trade order blocks automatically


1. **Clone the Repository:** Clone the repository to your local machine using the following command:

    ```bash
    git clone https://github.com/neavra/BlockTraderBot.git
    ```

2. **Configure Environment Variables:**

    - **(.env):** In the root directory, create a `.env` file with the following variables:

        ```env
        DATABASE_URL="postgresql://user:f994e125a74c839586f7172d0065bbed5e617433ce19ef27ae9bcb701cff9667@localhost:5432/blocktraderbot"

        POSTGRES_USER=user
        POSTGRES_PASSWORD=f994e125a74c839586f7172d0065bbed5e617433ce19ef27ae9bcb701cff9667
        POSTGRES_DB=blocktraderbot

        REDIS_URL="redis://localhost:6379"
        REDIS_PASSWORD=""
        
        EVENT_BUS_URL="amqp://guest:guest@localhost:5672/"
        ```

3. **Install Dependencies (with virtual env setup):**

    - Install dependencies:

        ```bash
        pip install -r requirements.txt
        ```

4. **Set Up Docker Container:** Ensure you have Docker Desktop installed and run the following command to spin up a Docker container for the Database:

    ```bash
    cd data
    docker-compose up --env-file ../.env -d
    ```

5. **Run Data layer:** Run the entry point for the Data Layer

    ```bash
    cd data
    python3 main.py
    ```

6. **Run Tests:** Run the unit & integration tests for the Data Layer

    ```bash
    cd data
    pytest tests/
    ```