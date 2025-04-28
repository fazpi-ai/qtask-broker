#!/bin/bash

# Script para iniciar el servidor M0 Broker API en modo PRODUCCIÓN.

# Salir inmediatamente si un comando falla
set -e

# 1. Establecer el entorno de la aplicación
export APP_ENV=production
echo "Estableciendo APP_ENV=production"

# 2. Activar el entorno virtual
# Asegúrate de que la ruta a tu venv sea correcta
VENV_PATH=".venv/bin/activate"
if [ -f "$VENV_PATH" ]; then
    echo "Activando entorno virtual: $VENV_PATH"
    source "$VENV_PATH"
else
    echo "Error: Entorno virtual no encontrado en $VENV_PATH"
    echo "Asegúrate de haber creado el venv en la carpeta .venv"
    exit 1
fi

# 3. Ejecutar Uvicorn para producción
# Lee el puerto desde las variables de entorno (cargadas por ConfigurationLoader desde .env.production o el entorno)
# Si PORT no está definido, ConfigurationLoader usará su default (3000), ajústalo si es necesario.
# Define el número de workers. Ajusta según los núcleos de tu CPU (ej. 2 * cores + 1)
WORKERS=${UVICORN_WORKERS:-4} # Usa la variable de entorno UVICORN_WORKERS si existe, sino default a 4
PORT=${PORT:-3000} # Usa la variable de entorno PORT si existe, sino default a 3000 (ConfigurationLoader también tiene su default)

echo "Iniciando servidor Uvicorn en modo producción en http://0.0.0.0:$PORT con $WORKERS workers..."
# Nota: Se elimina --reload y se añade --workers
uvicorn qtask_broker.main:app --host 0.0.0.0 --port $PORT --workers $WORKERS

# Nota: El script terminará aquí cuando detengas Uvicorn (o si es gestionado por un supervisor como systemd)
echo "Servidor detenido."

