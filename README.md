# Veflow-Project
 Ce projet contient le code en Python pour l'implémentation de la pipeline ETL:

 - Extract : On récupère les données des stations vélo Toulouse via l'API de JCDecaux. De plus, on lit un fichier json contenant les données statiques sur les différentes stations.
 - Transform : Nettoyage, filtrage des données
 - Load : Chargement dans une base de données PostgreSQL

```text
├── ETL
│   ├── extract_data.py        # Définition des fonctions pour extraire les données depuis l'API ou une source externe
│   ├── load_data.py           # Définition des fonctions pour charger les données transformées dans la base de données
│   └── transform_data.py      # Définition des fonctions pour nettoyer, filtrer ou transformer les données extraites
│
├── README.md                  # Documentation du projet : description, installation, usage
│
├── data
│   └── toulouse.json          # Fichier json contenant les données statiques des stations du réseau Vélo Toulouse
│
├── main.py                    # Script principal de la pipeline ETL (orchestration de l’extraction, transformation et chargement)
│
├── requirements.txt           # Liste des dépendances Python nécessaires pour exécuter le projet
│
├── setup_db.py                # Script Python pour initialiser/configurer la base de données
│
├── demo_data_manipulation.ipynb # Notebook Jupyter démonstration de l'utilisation des données.
│
├── Extract_data_test.csv      # Exemple de données récupérées grâce à la pipeline ETL sur une heure
│
└── test_api.ipynb             # Notebook de test Jupyter pour tester l’API
```

# Prérequis
Pour lancer cette pipeline ETL, il faut avoir installé:

- Python 3.x  
- `pip` (Python package installer) et `conda`  
- PostgreSQL

# Setup
1. Cloner le repo Git
```bash
git clone https://github.com/guiggzzz/Veflow-Project
cd ETL
```

2. Créer un environnement virtuel (optionnel)
```bash
conda create -n env_ETL (python=3.11)
conda activate env_ETL
```

4. Installer les packages requis

```bash
pip install -r requirements.txt
```


4. Mettre en place la base de données

Il faut d'abord définir son user et mot de passe PostgreSQL en tant que variable d'environnement, puis lancer le script Python.

```bash
export POSTGRES_USER="votre username postgreSQL"
export POSTGRES_PASSWORD="votre mot de passe postgreSQL"
python setup_db.py
```

# Utilisation
Pour réaliser un appel à l'API et stocker les données dans votre base de données PostgreSQL, exécuter la commande suivante:
```bash
python main.py
```

Le fichier demo_data_manipulation.ipynb est un Jupyter Notebook contenant un exemple de manipulation des données.











