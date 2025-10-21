import requests
import json
from typing import Dict, List, Any, Optional
import pandas as pd
import time


def extract(url: str, api_key: str, timeout: int = 10) -> Optional[List[Dict[str, Any]]]:
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

def read_csv(file_path: str, timeout: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Lit les données depuis un fichier CSV local.
    
    Args:
        file_path: Chemin vers le fichier CSV
        timeout: Timeout en secondes (défaut: 10s)
    
    Returns:
        Liste de dictionnaires contenant les données ou None en cas d'erreur
    """


    start_time = time.time()
    
    try:
        df = pd.read_csv(file_path)
        
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            print(f"❌ Timeout: lecture du fichier trop longue (>{timeout}s)")
            return None
        
        data = df.to_dict(orient='records')
        
        print(f"✅ Lecture réussie: {len(data)} enregistrements")
        return data
        
    except FileNotFoundError:
        print(f"❌ Erreur: fichier non trouvé à l'emplacement {file_path}")
        return None
        
    except pd.errors.EmptyDataError:
        print("❌ Erreur: le fichier CSV est vide")
        return None
        
    except Exception as e:
        print(f"❌ Erreur inattendue lors de la lecture du fichier: {e}")
        return None
    
def read_json(file_path: str, timeout: int = 10) -> Optional[List[Dict[str, Any]]]:
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


# if __name__ == "__main__":
#     # Configuration
#     API_KEY = "ba66e04fc0c6389242d8598aeee0906fe4b3d805"
#     BASE_URL = f"https://api.jcdecaux.com/vls/v1/stations?contract=toulouse&apiKey={API_KEY}"
    
#     # Extraction
#     data = extract(BASE_URL, API_KEY)
    
#     # Affichage
#     if data:
#         print(f"\nAperçu des données:\n")
#         print(json.dumps(data[:2], indent=4, ensure_ascii=False))
#     else:
#         print("\nÉchec de l'extraction")

