from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer

from flows.etl_web_to_s3 import main

docker_block = DockerContainer.load("real-estate-docker-block")

docker_dep = Deployment.build_from_flow(
    flow=main,
    name="docker-flow2",
    infrastructure=docker_block,
)


if __name__ == "__main__":
    docker_dep.apply()
