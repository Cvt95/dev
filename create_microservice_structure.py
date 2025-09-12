#!/usr/bin/env python3
"""
Script simple para crear solo la estructura de directorios de microservicios con Python.
"""

import os
import sys
from pathlib import Path


def create_microservice_directories(project_name: str, base_path: str = "."):
    """
    Crea solo los directorios de la estructura de microservicios.
    
    Args:
        project_name: Nombre del proyecto
        base_path: Ruta base donde crear el proyecto
    """
    project_path = Path(base_path) / project_name
    
    print(f"🚀 Creando estructura de directorios para: {project_name}")
    
    # Lista de directorios a crear
    directories = [
        # Estructura principal
        "src/api/v1/endpoints",
        "src/api/v2/endpoints", 
        "src/core",
        "src/models/database",
        "src/models/schemas",
        "src/services",
        "src/repositories",
        "src/utils",
        "src/workers/tasks",
        
        # Testing
        "tests/unit/test_services",
        "tests/unit/test_repositories", 
        "tests/unit/test_utils",
        "tests/integration/test_api",
        "tests/integration/test_database",
        "tests/e2e",
        "tests/fixtures",
        "tests/mocks",
        
        # Documentación
        "docs/api",
        "docs/architecture",
        "docs/development",
        
        # Scripts y configuración
        "scripts",
        "config/docker",
        "config/kubernetes",
        
        # Monitoreo
        "monitoring/prometheus",
        "monitoring/grafana/dashboards",
        "monitoring/jaeger",
        
        # CI/CD
        ".github/workflows",
    ]
    
    # Crear directorios
    for directory in directories:
        dir_path = project_path / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"📁 Creado: {directory}")
    
    # Crear archivos __init__.py en carpetas Python
    python_dirs = [
        "src",
        "src/api",
        "src/api/v1",
        "src/api/v1/endpoints",
        "src/api/v2", 
        "src/api/v2/endpoints",
        "src/core",
        "src/models",
        "src/models/database",
        "src/models/schemas",
        "src/services",
        "src/repositories",
        "src/utils",
        "src/workers",
        "src/workers/tasks",
        "tests",
        "tests/unit",
        "tests/integration",
        "tests/e2e",
        "tests/fixtures",
        "tests/mocks",
    ]
    
    for python_dir in python_dirs:
        init_file = project_path / python_dir / "__init__.py"
        init_file.write_text("")
        print(f"🐍 Creado: {python_dir}/__init__.py")
    
    print(f"\n✅ Estructura creada exitosamente en: {project_path}")
    print(f"📋 Total de directorios creados: {len(directories)}")
    print(f"📋 Total de archivos __init__.py creados: {len(python_dirs)}")


def main():
    """Función principal."""
    if len(sys.argv) < 2:
        print("❌ Error: Debes especificar el nombre del proyecto")
        print("Uso: python create_microservice_structure.py <nombre_proyecto>")
        print("Ejemplo: python create_microservice_structure.py user-service")
        sys.exit(1)
    
    project_name = sys.argv[1]
    
    # Validar nombre del proyecto
    if not project_name.replace("-", "").replace("_", "").isalnum():
        print("❌ Error: El nombre del proyecto solo puede contener letras, números, guiones y guiones bajos")
        sys.exit(1)
    
    create_microservice_directories(project_name)


if __name__ == "__main__":
    main()
