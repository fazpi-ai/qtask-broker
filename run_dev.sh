#!/bin/bash

# Script para iniciar el servidor M0 Broker API en modo desarrollo.

# Salir inmediatamente si un comando falla
set -e

# 1. Establecer el entorno de la aplicación
export APP_ENV=development
echo "Estableciendo APP_ENV=development"

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

# 3. Ejecutar Uvicorn
# Usamos el puerto 3000 por defecto, como en ConfigurationLoader.
# Puedes cambiarlo aquí o leerlo desde .env.development si prefieres más dinamismo.
PORT=3000
echo "Iniciando servidor Uvicorn en http://0.0.0.0:$PORT con recarga automática..."
uvicorn qtask_broker.main:app --host 0.0.0.0 --port $PORT --reload

# Nota: El script terminará aquí cuando detengas Uvicorn (Ctrl+C)
echo "Servidor detenido."

