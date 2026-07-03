# Deployment

This guide describes how to deploy a new version of Semaphore to a development or production server.

## Prerequisites

Before deploying, make sure you have:

- SSH access to the server.
- The Semaphore repository cloned on the server.
- A configured `.env` file in the project root.
- Docker and Docker Compose installed.
- Permission to access the repository and its tags.

---

# Initial Server Setup

These steps only need to be completed when setting up a new server.

## Clone the Repository

Clone the Semaphore repository onto the server.

```bash
git clone <repository-url>
cd semaphore
```

After cloning, verify that Git is working correctly.

```bash
git status
git fetch --tags
```

---

## Configure the Environment

Create a `.env` file in the repository root.

```bash
cp env.dist .env
```

Populate the file with the correct values for the server.

> **Note:** The `.env` file is not stored in Git. Obtain a copy from another developer or the project maintainers.

---

## Configure Nginx

Semaphore's FastAPI service is exposed through Nginx.

*** Ensure a CATS member is present when altering this ***

Add a location block similar to the following to the server's Nginx configuration:

```nginx
location /semaphore-api/ {
    proxy_pass http://localhost:<API_PORT>/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}
```

After updating the configuration:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

Verify that the API is accessible through the expected URL before deploying.

---

# Deploying Semaphore

Semaphore deployments are performed using the deployment script.

```bash
./tools/deploy.sh <git-tag>
```

Example:

```bash
./tools/deploy.sh v3.7.0
```

The deployment script automatically performs the following steps:

1. Creates a deployment log.
2. Stops any running Docker containers.
3. Fetches the latest Git tags.
4. Checks out the requested Git tag.
5. Builds the latest Docker images.
6. Starts the Docker containers.
7. Regenerates and installs the cron schedule.
8. Runs any required database migrations.
9. Waits for the containers to initialize.
10. Verifies that all containers pass their Docker health checks.

If every container reports a healthy status, the deployment is considered successful.

---

# Deployment Logs

Deployment logs are written to:

```text
logs/deployment/
```

Each log is timestamped and can be used to troubleshoot failed deployments.

---

# Verifying the Deployment

In an hour confirm that all containers are healthy and working as expected.
Document findings in our private wiki log: https://github.com/conrad-blucher-institute/semaphore-private-wiki/wiki

```bash
docker compose ps
```

You can also inspect the logs if needed.

```bash
docker compose logs -f
```

Verify that:

- the API is responding,
- scheduled models are executing,
- database migrations completed successfully.

---

# Cleaning Docker Images

Every deployment creates new Docker images.

Over time, unused images can consume a large amount of disk space on the server.

View Docker images:

```bash
docker images
```

Remove dangling images:

```bash
docker image prune
```

Remove all unused Docker resources:

```bash
docker system prune
```

> It is recommended to periodically clean unused Docker images on the Semaphore servers to avoid disk space issues.

---

# Rolling Back

To roll back to a previous release, redeploy an older Git tag.

Example:

```bash
./tools/deploy.sh v3.6.2
```

The deployment script will rebuild the containers using that version and run the appropriate database migration.