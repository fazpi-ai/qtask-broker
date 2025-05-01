# Guía de Configuración y Ejecución del Proyecto (usando uv)

Esta guía describe los pasos para configurar el entorno de desarrollo, instalar dependencias y ejecutar el servidor M0 Broker API usando `uv`.

## 1. Instalación de `uv` (Si no lo tienes)

`uv` es un instalador y gestor de entornos virtuales rápido para Python.

```bash
# Descarga e instala uv
curl -Ls [https://astral.sh/uv/install.sh](https://astral.sh/uv/install.sh) | bash

# Asegúrate de que el directorio bin de uv esté en tu PATH
# (El instalador usualmente te indica cómo hacerlo, puede variar según tu shell)
# Ejemplo para bash/zsh:
source $HOME/.local/bin/env
# O edita tu archivo de configuración de shell (~/.bashrc, ~/.zshrc, etc.) para añadirlo permanentemente.
# Puede que necesites reiniciar tu terminal o ejecutar:
source ~/.zshrc # O ~/.bashrc

# Verifica la instalación
uv --version
```

## 2. Configuración del Entorno Virtual

Es fundamental aislar las dependencias del proyecto usando un entorno virtual.

```bash
# Navega a la carpeta raíz del proyecto (m0-broker/)
cd ruta/a/m0-broker

# Crea un entorno virtual llamado '.venv' en la carpeta actual
uv venv

# Activa el entorno virtual recién creado
# (El comando varía según tu sistema operativo y shell)
# Ejemplo para bash/zsh en Linux/macOS:
source .venv/bin/activate
# Ejemplo para PowerShell en Windows:
# .\.venv\Scripts\Activate.ps1
# Ejemplo para cmd.exe en Windows:
# .\.venv\Scripts\activate.bat

# Tu terminal debería ahora mostrar (.venv) al inicio de la línea.
```

## 3. Gestión de Dependencias

Instala las bibliotecas Python requeridas por el proyecto.

```bash
# Asegúrate de que el entorno virtual esté activado

# Instala las dependencias listadas en requirements.txt
uv pip install -r requirements.txt

# --- Si necesitas añadir una nueva dependencia (ejemplo: 'requests') ---
# uv pip install requests

# --- Si necesitas actualizar el archivo requirements.txt después de añadir/quitar dependencias ---
# uv pip freeze > requirements.txt
```

## 4. Ejecución del Servidor

Existen scripts para facilitar la ejecución en diferentes entornos.

### Modo Desarrollo

Este modo es ideal mientras estás codificando, ya que recarga automáticamente el servidor cuando detecta cambios en los archivos.

```bash
# Asegúrate de que el entorno virtual esté activado
# Asegúrate de estar en la carpeta raíz del proyecto (m0-broker/)

# Otorga permisos de ejecución al script (solo necesitas hacerlo una vez)
chmod +x run_dev.sh

# Ejecuta el script de desarrollo
./run_dev.sh
```
Esto establecerá `APP_ENV=development`, activará el entorno (si no lo estaba ya) e iniciará `uvicorn` en el puerto configurado (usualmente 3000) con `--reload`.

### Modo Producción

Este modo está optimizado para despliegues. No usa recarga automática y puede utilizar múltiples workers.

```bash
# Asegúrate de que el entorno virtual esté activado
# Asegúrate de estar en la carpeta raíz del proyecto (m0-broker/)
# Asegúrate de tener un archivo .env.production con la configuración adecuada

# Otorga permisos de ejecución al script (solo necesitas hacerlo una vez)
chmod +x run_prod.sh

# Ejecuta el script de producción
./run_prod.sh
```
Esto establecerá `APP_ENV=production`, activará el entorno e iniciará `uvicorn` con la configuración de producción (puerto, número de workers).

## 5. Ejecución de Tests Unitarios

Para verificar que los componentes funcionan correctamente:

```bash
# Asegúrate de que el entorno virtual esté activado
# Asegúrate de estar en la carpeta raíz del proyecto (m0-broker/)

# Ejecutar todos los tests descubiertos en la carpeta 'tests/'
python -m unittest discover -v

# O ejecutar los tests de un archivo específico (ejemplo):
# python -m unittest tests.test_config -v

```

## 6. Ejecución del Cliente

Para verificar que el cliente se conecta correctamente al broker:

```bash
export QTASK_BROKER_URL="http://127.0.0.1:3000"
python examples/basic.py