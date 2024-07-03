# Projeto A2

## Pré-requisitos

Antes de executar o projeto, certifique-se de ter os seguintes pré-requisitos instalados:

1. **Docker**: Ferramenta para criar e gerenciar contêineres. Siga as instruções de instalação no [site oficial do Docker](https://docs.docker.com/get-docker/).

2. **Docker Compose**: Ferramenta para definir e executar aplicativos Docker multi-contêiner. Siga as instruções de instalação no [site oficial do Docker Compose](https://docs.docker.com/compose/install/).

Para verificar se ambos estão instalados corretamente, execute os seguintes comandos no terminal:

```bash
docker --version
docker-compose --version
```

## Instruções para Executar o Projeto

Para iniciar o projeto, abra um terminal na pasta do repositório e execute o seguinte comando:

```bash
docker-compose up --build -d
```

Em seguida, inicie o cliente:

```bash
docker-compose up
```

Você pode usar os seguintes comandos para verificar o status do servidor:

```bash
docker ps #Para ver os containers ativos
```

```bash
docker-compose logs [nome_do_container]
```
