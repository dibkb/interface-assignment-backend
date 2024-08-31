### The backend deployed is available as 
[https://interface-assignment-server.dibkb.xyz/](https://interface-assignment-server.dibkb.xyz/)

# How to Run This Project Locally

Follow the steps below to clone the repository and start the server using Docker Compose.

## Prerequisites

Ensure you have the following installed on your machine:

- [Git](https://git-scm.com/)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

## Steps to Run Locally

1. **Clone the repository**

   Open your terminal and run the following command to clone the repository:

   ```bash
   git clone -b main https://github.com/dibkb/interface-assignment-backend 
   ```
   
2. **Navigate into the project directory**

   In the terminal and run the following command to navigate the project directory:

   ```bash
   cd interface-assignment-backend 
   ```
3. **Build and Start the Docker Containers**

   Use Docker Compose to build the Docker image and start the containers:

   ```bash
   docker compose up --build
   ```

4. **Access the Application**

   Once the containers are up and running, the server will be accessible at:

   ```bash
   http://localhost:8000
   ```

4. **Setup the frontend**

   Once the backend setup is complete, navigate to the frontend and setup the NextJs app locally:
   - [https://github.com/dibkb/interface-frontend](https://github.com/dibkb/interface-frontend)



### CI Pipeline

| Step | Description |
|---|---|
| Start CI | Begin the CI process. |
| Install Python 3.11 | Install Python version 3.11. |
| Upgrade pip | Upgrade the pip package manager to the latest version. |
| Install Dependencies | Install the required project dependencies. |
| Install autopep8 | Install the autopep8 code formatter. |
| Format Code | Automatically format the code using autopep8. |
| Install flake8 | Install the flake8 linter. |
| Lint Code | Check for potential code quality issues using flake8. |
| End CI | Conclude the CI process. |

### CD Pipeline

| Step | Description |
|---|---|
| Start CD | Begin the CD process. |
| Setup Docker | Install and configure Docker. |
| Docker Login | Log in to your Docker registry (e.g., Docker Hub). |
| Build Docker Image | Build a Docker image containing your application. |
| Push Docker Image | Push the Docker image to your registry. |
| SSH to EC2 | Connect to your EC2 instance via SSH. |
| Copy docker-compose.yml | Copy the `docker-compose.yml` file to the EC2 instance. |
| Pull Latest Image | Pull the latest Docker image from your registry. |
| Docker Compose Down | Stop and remove existing containers and networks. |
| Docker Compose Up | Start and run the application using Docker Compose. |
| End CD | Conclude the CD process. |


![CI/CD pipeline for the fast api server](https://github.com/user-attachments/assets/0c5fd979-19fd-46c3-bf3c-945e4c9a5e00)

# ETL Pipeline Process

1. Client uploads files to FastAPI server.

2. FastAPI server initializes the database.

3. FastAPI server logs the start of file processing.

4. FastAPI server processes the files using a FileProcessor component.

5. FileProcessor reads the MTR and Payment files.

6. FileProcessor checks the availability of a service in a loop until it is successful.

7. FileProcessor transforms the data in the MTR and Payment files.

8. FileProcessor merges the transformed dataframes and applies a tolerance check.

9. If the data is good, FileProcessor returns the processed data to FastAPI server. If the data is bad, FileProcessor returns an error to FastAPI server.

10. FastAPI server logs the completion of file processing.

11. FastAPI server saves the processed data to the database.

12. FastAPI server returns a response to the client.


![ELT porcess pipeline](https://github.com/user-attachments/assets/daa29896-8b2d-4c00-ab12-653efcea6ee8)
