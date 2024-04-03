-- entrypoint-creation.sql

-- Grant privileges to core user on existing database
CREATE USER ${CORE_USER} WITH PASSWORD '${CORE_PASSWORD}';
-- The ISSUE right now is that we can't read these: ${CORE_USER} from the .env file
-- because sql isn't built like that, so the current solution idea is to write a preprocess
-- script but- i'm kinda confused on it
GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${CORE_USER};
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO ${CORE_USER};
GRANT CREATE ON SCHEMA public TO ${CORE_USER};

-- Grant privileges to api user on existing database
CREATE USER ${API_USER} WITH PASSWORD '${API_PASSWORD}';
GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${API_USER};
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO ${API_USER};
