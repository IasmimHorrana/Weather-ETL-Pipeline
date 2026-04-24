# conftest.py — Configuração raiz do pytest.
# Este arquivo faz o pytest adicionar a raiz do projeto ao sys.path,
# permitindo que os testes importem módulos de src/ sem precisar de
# configurações extras ou 'pip install -e .'
import sys
from pathlib import Path

# Adiciona a raiz do projeto ao path de importação
sys.path.insert(0, str(Path(__file__).parent))
