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

   ```bash
   https://github.com/dibkb/interface-frontend
   ```



