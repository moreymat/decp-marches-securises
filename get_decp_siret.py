"""Récupérer les DECP sur le site marches-securises"""

import argparse
import json
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests


URL = "https://www.marches-securises.fr/entreprise/"

PARAMS = {
    "module": "liste_donnees_essentielles",
    "presta": ";services;travaux;fournitures;autres",
    "date_cloture_type": "0",
    "donnees_essentielles": "1",
}

FORM_DATA = {
    "module": "liste_consultations",
    "submit.x": "76",
    "submit.y": "13",
    "search": "table_ms",
    "presta_au": "1",
    # "siret_pa1"
    "objet": "",
    # "date_deb_ms"
    # "date_fin_ms"
    "dep_liste": "",
    "siret_pa": "",
    "type_marche": "MARCHE",
    "type_procedure": "",
    "date_deb": "",
    "date_fin": "",
    "ref_ume": "",
    "cpv_et": "",
    "rs_oe": "",
    "texte_libre": "",
}


def download_json_from_results_page(html_page: str):
    """Télécharge les DECP au format JSON dans une page de résultats.

    Renvoie une liste de DECP au format JSON.

    Parameters
    ----------
    html_page: str
        Page HTML de résultats du site

    Returns
    -------
    json_entries: List[JSON_DECP]
        Liste d'entrées DECP en JSON
    """
    # analyser la page de 10 résultats pour extraire les liens vers les
    # fichiers JSON
    r_soup = BeautifulSoup(html_page, "html.parser")
    links_json = r_soup.find_all(title="Télécharger au format Json")
    # print(links_json)
    # print(len(links_json))

    # récupérer chaque fichier JSON et l'analyser
    json_entries = []
    for link in links_json:
        url_json = urljoin("https://www.marches-securises.fr", link["href"])
        r_json = requests.get(url_json)
        # print(r_json.text)
        # la cible n'est pas un fichier JSON, mais une page HTML dont le body
        # contient l'entrée au format JSON...
        # 1. le HTML renvoyé est incorrect: il n'y a de <body> et la
        # balise <html> n'est pas fermée... le plus simple est de jeter
        # tout ce qui a été ajouté avant l'entrée JSON, donc jusqu'au
        # </head> inclus
        # 2. le HTML est en UTF-8 mais le JSON est codé en ASCII, avec des
        # backslashes supplémentaires pour les caractères non-ASCII, on prend
        # donc le ".content" brut pour le décoder avec
        # "raw_unicode_escape"
        # (réf: <https://docs.python.org/3/library/codecs.html> )
        entry = r_json.content.decode("raw_unicode_escape").split("</head>")[1].strip()
        json_entry = json.loads(entry)
        json_entries.append(json_entry)
    return json_entries


def get_next_page(html_page: str) -> Optional[str]:
    """Extrait l'URL de la page de résultats suivante.

    Renvoie None s'il n'y a pas de page suivante.

    Parameters
    ----------
    html_page: str
        Page de résultats (HTML) actuelle.

    Returns
    -------
    next_page: str or None
        URL de la page de résultats (HTML) suivante.
    """
    r_soup = BeautifulSoup(html_page, "html.parser")
    pagination_data = r_soup.find("div", class_="pagination_data")
    # le lien vers la prochaine page est un <a> dont le texte est "<strong>&gt;&gt;</strong>"
    # donc on cherche le texte ">>", on remonte au <strong>, on remonte au <a> et on récupère
    # l'adresse href
    try:
        next_page = pagination_data.find(text=">>").parent.parent["href"]
    except KeyError:
        return None
    else:
        return next_page


def scrape_decp(args_data: Dict[str, str]):
    """Récupère les DECP depuis marches-securises.

    Une requête est définie par un SIRET acheteur et des dates de début et fin.
    Les résultats sont une liste d'entrées DECP au format JSON.

    Parameters
    ----------
    args_data: Dict[str, str]
        Paramètres de la requête: SIRET et dates de début et fin.

    Returns
    -------
    json_entries: List[JSON_DECP]
        Liste d'entrées DECP au format JSON.
    """
    # liste de stockage des résultats
    json_entries = []
    # envoyer la requête par le formulaire
    # avec les paramètres: SIRET et dates de début et fin
    FORM_DATA.update(args_data)
    r = requests.post(URL, data=FORM_DATA, params=PARAMS)
    # on reçoit la 1re page de résultats
    result_page = r.text
    print(f"Page initiale: {r.url}")
    while True:
        # récupérer les JSON de la page de résultats courante (10 max)
        new_entries = download_json_from_results_page(result_page)
        json_entries.extend(new_entries)
        # extraire l'adresse de la prochaine page
        next_page = get_next_page(result_page)
        if next_page is None:
            break
        print(f"Page suivante: {next_page}")
        result_page = requests.get(next_page).text
    return json_entries


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Récupère les DECP d'un acheteur sur marches-securises.fr"
    )
    # ex: Croix: "21590163800019", "2019-01-01", "2021-12-31"
    parser.add_argument("siret", help="SIRET de l'acheteur")
    parser.add_argument("datedeb", help="Date de début")
    parser.add_argument("datefin", help="Date de fin")
    args = parser.parse_args()
    # fichier de résultats
    fp_out = Path(
        "data", "processed", f"decp_{args.siret}_{args.datedeb}_{args.datefin}.json"
    )
    # paramètres spécifiques à cette exécution
    args_data = {
        "siret_pa1": args.siret,
        "date_deb_ms": args.datedeb,
        "date_fin_ms": args.datefin,
    }
    # extraction des résultats
    json_entries = scrape_decp(args_data)
    # export dans un fichier JSON
    with open(fp_out, mode="w") as f_out:
        json.dump({"marches": json_entries}, f_out, ensure_ascii=False, indent=2)
