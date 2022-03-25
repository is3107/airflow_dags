from fileinput import filename
from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.client import User
from diagrams.digitalocean.network import Domain
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.network import Nginx
from diagrams.gcp.analytics import Bigquery
from diagrams.gcp.compute import Functions


with Diagram("Architectural Overview", show=False, outformat="jpg", filename="./images/architectural_overview"):
    with Cluster("Airflow Developers"):
        airflow_user_a = User("Developer A")
        airflow_user_b = User("Developer B")

    end_user = User("End-User")

    with Cluster("Dev"):
        with Cluster("Frontend"):
            web_dev = Domain("Airflow Web App")
        with Cluster("VM"):

            with Cluster("Reverse Proxy"):
                nginx_dev = Nginx("NGINX")

            with Cluster("Metadata Store"):
                postgres_dev = PostgreSQL("PostgreSQL")
            
            with Cluster("Orchestrator"):
                airflow_dev = Airflow("Airflow")

            nginx_dev >> Edge() << airflow_dev 
            postgres_dev >> Edge() << airflow_dev



    with Cluster("Prod"):
        with Cluster("Frontend"):
            web_prod = Domain("Airflow Web App")

        with Cluster("VM"):
            with Cluster("Metadata Store"):
                postgres_prod = PostgreSQL("PostgreSQL")

            with Cluster(" Reverse Proxy"):
                nginx_prod = Nginx("NGINX")
            
            with Cluster("Orchestrator"):
                airflow_prod = Airflow("Airflow")

            postgres_prod >> Edge() << airflow_prod
            nginx_prod >> Edge() << airflow_prod 
            

    with Cluster("Data Warehouse"):
        with Cluster("Database"):
            bigquery = Bigquery("Bigquery")

        with Cluster("Compute"):
            bigquery_compute = Bigquery("Bigquery")

        with Cluster("Compute"):
            gcp_functions = Functions("Cloud Functions")

        with Cluster("Serving Layer"):
            bigquery_serving = Bigquery("Bigquery")
        

        all_compute = [bigquery_compute, gcp_functions]
        all_compute >> Edge() << bigquery
        bigquery >> bigquery_serving


    airflow_user_a >> web_dev
    airflow_user_b >> web_prod
    web_dev >> nginx_dev
    web_prod >> nginx_prod

    airflow_dev >> bigquery_compute
    airflow_prod >> gcp_functions

    bigquery_serving >> end_user



