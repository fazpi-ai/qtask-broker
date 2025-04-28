#!/bin/bash

# Script para iniciar el servidor M0 Broker API en modo PRODUCCIÓN (versión para contenedor).

# Salir inmediatamente si un comando falla
set -e

# 1. Establecer el entorno de la aplicación
export APP_ENV=production
echo "Estableciendo APP_ENV=production"

# 2. Activar el entorno virtual --- ¡SECCIÓN ELIMINADA! ---
# Ya no es necesario activar un venv dentro del contenedor,
# las dependencias se instalan globalmente en la imagen Docker.
# VENV_PATH=".venv/bin/activate"
# if [ -f "$VENV_PATH" ]; then
#     echo "Activando entorno virtual: $VENV_PATH"
#     source "$VENV_PATH"
# else
#     echo "Error: Entorno virtual no encontrado en $VENV_PATH"
#     echo "Asegúrate de haber creado el venv en la carpeta .venv"
#     exit 1
# fi
echo "Saltando activación de venv (no necesaria en el contenedor)."


# 3. Ejecutar Uvicorn para producción
# Lee el puerto desde las variables de entorno (inyectadas por Kubernetes o defaults)
# Define el número de workers. Ajusta según los núcleos de tu CPU (ej. 2 * cores + 1)
WORKERS=${UVICORN_WORKERS:-4} # Usa la variable de entorno UVICORN_WORKERS si existe, sino default a 4
# PORT es establecido por Kubernetes via env var en el Deployment YAML
PORT=${PORT:-3000} # El valor del YAML (3000) tendrá precedencia

echo "Iniciando servidor Uvicorn en modo producción en http://0.0.0.0:$PORT con $WORKERS workers..."
# Nota: Se elimina --reload y se añade --workers
uvicorn qtask_broker.main:app --host 0.0.0.0 --port $PORT --workers $WORKERS

# Nota: El script terminará aquí cuando detengas Uvicorn
echo "Servidor detenido."