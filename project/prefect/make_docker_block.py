from prefect.infrastructure.docker import DockerContainer

## alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="shixi99/resetl:project",  
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

docker_block.save("real-estate-docker-block", overwrite=True)