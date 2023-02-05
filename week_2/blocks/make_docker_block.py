from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer(
    image = 'shixi99/prefect:prefect_taxi_data',
    image_pull_policy = 'ALWAYS',
    auto_remove=True
)

docker_block.save('docker-taxi-data', overwrite=True)