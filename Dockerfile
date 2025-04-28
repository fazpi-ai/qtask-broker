# 1. Usa una imagen base de Python oficial
# Elige una versión específica y preferiblemente 'slim' para menor tamaño
FROM python:3.10-slim

# 2. Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. (Opcional pero recomendado) Establece la variable de entorno aquí
# El script run_prod.sh también la establece, pero ponerla aquí es más explícito
# para la construcción de la imagen y cualquier paso intermedio.
ENV APP_ENV=production
ENV PYTHONUNBUFFERED=1

# 4. Copia el archivo de dependencias
COPY requirements.txt .

# 5. Instala las dependencias
# --no-cache-dir reduce el tamaño de la imagen
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copia todo el código de tu aplicación al directorio de trabajo
# Asegúrate de que tu contexto de build (.) incluya qtask_broker/, main.py, run_prod.sh, .env.production (si lo necesitas DENTRO de la imagen)
# Es mejor manejar secrets/config via variables de entorno o volúmenes en producción real.
COPY ./qtask_broker ./qtask_broker
COPY ./run_prod.sh .
# Si necesitas main.py directamente en /app (aunque tu script llama a qtask_broker.main)
# COPY main.py .
# Si necesitas los archivos .env DENTRO de la imagen (menos seguro):
# COPY .env.production .

# 7. Haz ejecutable el script de producción
RUN chmod +x ./run_prod.sh

# 8. Expón el puerto que usa tu aplicación (el script usa PORT o 3000 por defecto)
# Esto es documentación; no abre el puerto realmente. Necesitas -p al correr `docker run`.
EXPOSE 3000

# 9. Define el comando por defecto que se ejecutará cuando inicie el contenedor
# Ejecuta tu script de producción.
CMD ["./run_prod.sh"]

# --- Consideraciones Adicionales (Buenas Prácticas) ---
# - Multi-stage builds: Para reducir el tamaño final de la imagen.
# - Usuario no-root: Por seguridad, crea un usuario y corre la app con él.
#   RUN addgroup --system app && adduser --system --ingroup app app
#   USER app
# - Manejo de secretos/configuración: Evita copiar archivos .env a la imagen. Usa variables de entorno
#   inyectadas al correr el contenedor o sistemas de gestión de configuración/secretos.