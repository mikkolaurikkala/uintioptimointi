"""Tämä moduuli hakee Uimaliiton Tempus Openista ensin hakukriteereillä valittujen
uimarien nimilistan ja tallentaa sen tiedostoon. Sitten toinen funktio lukee nimilistasta
kaikkien ID-numerot, hakee niille tilastoajat ja tallentaa ne uimarikohtaisiin tiedostoihin.
"""

import csv
import urllib.parse
import json
import requests
import bs4
import pandas as pd

DATAHAKEMISTO = "Data/"
PARAMETRITIEDOSTO = "parametrit.json"
NIMILISTATIEDOSTO = DATAHAKEMISTO + "uimarit.csv"
EROTIN = ";"
PERUSURL = "https://www.tempusopen.fi/index.php?r=Swimmer"
# p = parametrit.Parametrit()

with open(PARAMETRITIEDOSTO, "r", encoding="utf-8") as f:
    parametrit_dict = json.load(f)


# def nimilistaparametrit() -> dict[str, str]:
#     with open(PARAMETRITIEDOSTO, "r", encoding="utf-8") as f:
#         json_data = json.load(f)
#     return json_data.get("nimilistaparametrit")


# def yhden_uimarin_parametrit() -> dict[str, str]:
#     with open(PARAMETRITIEDOSTO, "r", encoding="utf-8") as f:
#         json_data = json.load(f)
#     return json_data.get("yhden_uimarin_parametrit")


def rakenna_haku_url() -> str:
    """Lue hakuparametrit ja rakenna URL, jolla haetaan haun
    tulokset Tempuksesta.

    Returns:
        str: URL
    """
    parametrit_urliin = {
        f"Swimmer[{key}]": value
        for key, value in parametrit_dict["nimilistaparametrit"].items()
    }
    parametrit_enkoodattu = urllib.parse.urlencode(parametrit_urliin)
    return f"{PERUSURL}/index&{parametrit_enkoodattu}"


def hae_uimarilista():
    """Hae uimarien nimilista Tempuksesta hakuparametrien mukaan.
    Siivoa tiedot ja tallenna csv-tiedostoon.
    """

    vastaus = requests.get(rakenna_haku_url(), timeout=60)
    html_olio = bs4.BeautifulSoup(vastaus.text, "html.parser")
    taulukko = html_olio.find("table")
    otsikot = [th.text.strip() for th in taulukko.find_all("th")]
    otsikot.insert(0, "ID")  # Lisää otsikoihin ekaksi ID

    data = []
    for rivi in taulukko.find_all("tr")[1:]:  # Jätä otsikkorivi pois
        cells = rivi.find_all("td")
        if not cells:
            continue

        # Poimi etunimi ja jätä ankkurina oleva "Swimmer" pois
        etunimi = cells[0].text.replace("Swimmer", "").strip()

        # Poimi ID ekan solun ankkuri-tagin sisältä
        id_tag = cells[0].find("a")
        uimarin_id = id_tag["href"].split("id=")[-1] if id_tag else None

        # Poimi loput solut sellaisenaan ja rakenna rivi. Taulukon
        # suurennuslasisymboli jättää loppuun tyhjän sarakkeen, mutta se ei
        # haittaa.
        rivin_data = [uimarin_id, etunimi] + [cell.text.strip() for cell in cells[1:]]
        data.append(rivin_data)

    pd.DataFrame(data, columns=otsikot).to_csv(
        NIMILISTATIEDOSTO, sep=EROTIN, index=False
    )
    print(f"Nimilista haettu ja tallennettu tiedostoon {NIMILISTATIEDOSTO}.")


def hae_uimarin_ajat(uimarin_id: int, alkupvm: str):
    """Hae yhden uimarin tilastoajat Tempuksesta ja tallenna ne csv-tiedostoon.
    Tilastosivulla on kaksi taulukkoa, yksi kummallekin ratapituudelle.
    Yhdistä taulukot samaan ja lisää sarake "Rata".

    Args:
        uimarin_id (int):   ID-numero Tempuksessa
        alkupvm (str):      Päivämäärä, josta alkaen ajat haetaan, muodossa 2023-01-01
    """

    id_url = (
        PERUSURL
        + f"/view&id={uimarin_id}"
        + f"&ResultSwim[start_date]={alkupvm}"
        + "&ResultSwim[end_date]=&ResultSwim[besttimes]=1"
    )
    vastaus = requests.get(id_url, timeout=60)
    html_olio = bs4.BeautifulSoup(vastaus.text, "html.parser")
    rataotsikot = [h3.text.strip() for h3 in html_olio.find_all("h3")]
    taulukot = html_olio.find_all("table")
    dataframet = []

    # Sivulla on 1 tai 2 h3-otsikkoa ja taulukkoa. Käy ne läpi.
    for rata, taulukko in zip(rataotsikot, taulukot):
        taulukon_otsikot = [th.text.strip() for th in taulukko.find_all("th")]
        rows = []

        # Kerää rivit taulukosta
        for row in taulukko.find_all("tr")[1:]:  # Jätä otsikkorivi pois
            cells = row.find_all("td")
            rows.append([cell.text.strip() for cell in cells])

        # Muunna dataframeksi ja lisää Rata-sarake
        try:
            df = pd.DataFrame(rows, columns=taulukon_otsikot)
        except ValueError:  # Dataframessa ei ole tilastoaikoja
            pass
        else:
            df["Rata"] = rata
            dataframet.append(df)

    tiedoston_nimi = f"id{uimarin_id}_tulokset.csv"
    pd.concat(dataframet, ignore_index=True).to_csv(
        DATAHAKEMISTO + tiedoston_nimi, sep=EROTIN, index=False
    )
    print(f"Ajat haettu ja tallennettu tiedostoon {tiedoston_nimi}.")


def hae_kaikkien_ajat():
    """Lue nimilistatiedostosta ID:t ja hae yksitellen niiden uimarien tilastoajat.
    Tallenna erillisiin CSV-tiedostoihin.
    """
    alkupvm = parametrit_dict["yhden_uimarin_parametrit"]["start_date"]
    with open(NIMILISTATIEDOSTO, mode="r", encoding="utf-8") as tied:
        csv_lukija = csv.reader(tied, delimiter=EROTIN)
        next(csv_lukija)  # Skippaa otsikkorivi
        for rivi in csv_lukija:
            uimarin_id = rivi[0]  # Eka sarake on ID
            hae_uimarin_ajat(uimarin_id, alkupvm)


if __name__ == "__main__":
    hae_uimarilista()
    hae_kaikkien_ajat()
