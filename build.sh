#!/bin/bash

# Salir inmediatamente si un comando falla
set -e

# --- Configuración ---
# Usuario de Docker Hub (confirmado)
DOCKER_USER="cloudcitycolombia"
# Nombre del repositorio en Docker Hub (confirmado)
IMAGE_REPO="qtask-broker"
# Puedes usar 'latest' o una etiqueta más específica (ej. versión, fecha, git commit hash)
TAG="latest"
# --------------------

# Nombre completo de la imagen en Docker Hub
FULL_IMAGE_NAME="$DOCKER_USER/$IMAGE_REPO:$TAG"

# Plataforma de destino para la imagen
TARGET_PLATFORM="linux/amd64"

echo "--- Preparando para construir y publicar la imagen ---"
echo "Usuario Docker Hub: $DOCKER_USER"
echo "Repositorio        : $IMAGE_REPO"
echo "Tag                : $TAG"
echo "Imagen Completa    : $FULL_IMAGE_NAME"
echo "Plataforma Destino : $TARGET_PLATFORM"
echo "Directorio Build   : $(pwd)"
echo "---------------------------------------------------"
echo ""

# Solicitar la contraseña de Docker Hub de forma segura
echo -n "Introduce la contraseña de Docker Hub para el usuario '$DOCKER_USER': "
read -s DOCKER_PASSWORD # -s oculta la entrada
echo "" # Nueva línea después de introducir la contraseña

# Iniciar sesión en Docker Hub usando la contraseña de forma segura
echo "Iniciando sesión en Docker Hub..."
echo "$DOCKER_PASSWORD" | docker login --username "$DOCKER_USER" --password-stdin

# Liberar la variable de contraseña de la memoria (buena práctica)
unset DOCKER_PASSWORD

echo ""
echo "Construyendo la imagen para $TARGET_PLATFORM y subiéndola a Docker Hub..."
echo "Esto puede tardar un poco..."

# Usar buildx para construir para la plataforma específica y subirla
# Docker Desktop en Mac M1/M2 generalmente usa buildx por defecto.
# --platform especifica la arquitectura de destino.
# -t etiqueta la imagen.
# --push construye y sube la imagen en un solo paso.
# '.' indica que el Dockerfile está en el directorio actual.
docker buildx build --platform "$TARGET_PLATFORM" \
    -t "$FULL_IMAGE_NAME" \
    --push \
    .

echo ""
echo "--- ¡Éxito! ---"
echo "La imagen $FULL_IMAGE_NAME ha sido construida para $TARGET_PLATFORM y publicada en Docker Hub."
echo "----------------"

# (Opcional) Cerrar sesión de Docker Hub
# echo "Cerrando sesión de Docker Hub..."
# docker logout

exit 0