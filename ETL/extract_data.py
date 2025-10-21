import requests
import json
from typing import Dict, List, Any, Optional
import pandas as pd
import time


def extract_status(url: str, api_key: str, timeout: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Extrait les données depuis l'API JCDecaux.
    
    Args:
        url: URL de base de l'API
        api_key: Clé API pour l'authentification
        timeout: Timeout en secondes (défaut: 10s)
    
    Returns:
        Liste de dictionnaires contenant les données ou None en cas d'erreur
    """
    if not url or not api_key:
        print("❌ Erreur: L'URL et l'api_key ne peuvent pas être vides")
        return None
    

    
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        
        print(f"✅ Extraction réussie: {len(data)} enregistrements")
        return data
        
    except requests.exceptions.Timeout:
        print(f"❌ Timeout: requête trop longue (>{timeout}s)")
        return None
        
    except requests.exceptions.ConnectionError:
        print("❌ Erreur de connexion internet")
        return None
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ Erreur HTTP {response.status_code}: {e}")
        return None
        
    except json.JSONDecodeError:
        print("❌ Erreur: réponse JSON invalide")
        return None
        
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        return None

    
def read_stations_toulouse(file_path: str, timeout: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Lit les données depuis un fichier JSON local.
    
    Args:
        file_path: Chemin vers le fichier JSON
        timeout: Timeout en secondes (défaut: 10s)
    
    Returns:
        Liste de dictionnaires contenant les données ou None en cas d'erreur
    """
    start_time = time.time()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.loads(f.read())

        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            print(f"❌ Timeout: lecture du fichier trop longue (>{timeout}s)")
            return None
        
        print(f"✅ Lecture réussie: {len(data)} enregistrements")
        return data
        
    except FileNotFoundError:
        print(f"❌ Erreur: fichier non trouvé à l'emplacement {file_path}")
        return None
        
    except json.JSONDecodeError:
        print("❌ Erreur: fichier JSON invalide")
        return None
        
    except Exception as e:
        print(f"❌ Erreur inattendue lors de la lecture du fichier: {e}")
        return None

