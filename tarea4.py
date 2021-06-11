import pandas as pd
import requests
import xml.etree.ElementTree as ET
import gspread
from gspread_dataframe import set_with_dataframe

gc = gspread.service_account(
    filename='taller-tarea-4-316303-9047df5b1f74.json')
sh = gc.open_by_key('1FhJn1bBvo3Ph_1YcZLyjooiIwPw0yI1ZBkf85CP-Ozs')

worksheet = sh.get_worksheet(0)

worksheet.clear()

COUNTRY_DICT = {"CHILE": "CHL", "ARGENTINA": "ARG",
                "BRASIL": "BRA", "EEUU": "USA", "ALEMANIA": "DEU", "FRANCIA": "FRA"}

xml_chl = requests.get(
    f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{COUNTRY_DICT["CHILE"]}.xml')
xml_arg = requests.get(
    f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{COUNTRY_DICT["ARGENTINA"]}.xml')
xml_bra = requests.get(
    f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{COUNTRY_DICT["BRASIL"]}.xml')
xml_usa = requests.get(
    f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{COUNTRY_DICT["EEUU"]}.xml')
xml_deu = requests.get(
    f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{COUNTRY_DICT["ALEMANIA"]}.xml')
xml_fra = requests.get(
    f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{COUNTRY_DICT["FRANCIA"]}.xml')

etree_chl = ET.fromstring(xml_chl.content)
etree_arg = ET.fromstring(xml_arg.content)
etree_bra = ET.fromstring(xml_bra.content)
etree_usa = ET.fromstring(xml_usa.content)
etree_deu = ET.fromstring(xml_deu.content)
etree_fra = ET.fromstring(xml_fra.content)

all_etree = [etree_chl, etree_arg, etree_bra, etree_usa, etree_deu, etree_fra]

indicator_list = ["Number of deaths",
                  "Number of infant deaths",
                  "Number of under-five deaths",
                  "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
                  "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)",
                  "Estimates of number of homicides",
                  "Crude suicide rates (per 100 000 population)",
                  "Mortality rate attributed to unintentional poisoning (per 100 000 population)",
                  "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
                  "Estimated road traffic death rate (per 100 000 population)",
                  "Estimated number of road traffic deaths",
                  "Mean BMI (kg/m&#xb2;) (crude estimate)",
                  "Mean BMI (kg/m&#xb2;) (age-standardized estimate)",
                  "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)",
                  "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)",
                  "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)",
                  "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)",
                  "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)",
                  "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)",
                  "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)",
                  "Estimate of daily cigarette smoking prevalence (%)",
                  "Estimate of daily tobacco smoking prevalence (%)",
                  "Estimate of current cigarette smoking prevalence (%)",
                  "Estimate of current tobacco smoking prevalence (%)",
                  "Mean systolic blood pressure (crude estimate)",
                  "Mean fasting blood glucose (mmol/l) (crude estimate)",
                  "Mean Total Cholesterol (crude estimate)"
                  ]


def getAllRows(etree_list, indicator_list):
    rows = []
    for tree in etree_list:
        for node in tree:
            indicator = node.find("GHO").text if node.find(
                "GHO") is not None else None
            if indicator in indicator_list:
                country = node.find("COUNTRY").text if node.find(
                    "COUNTRY") is not None else None
                sex = node.find("SEX").text if node.find(
                    "SEX") is not None else None
                ghecauses = node.find("GHECAUSES").text if node.find(
                    "GHECAUSES") is not None else None
                age_group = node.find("AGEGROUP").text if node.find(
                    "AGEGROUP") is not None else None
                year = node.find("YEAR").text if node.find(
                    "YEAR") is not None else None
                numeric = node.find("Numeric").text if node.find(
                    "Numeric") is not None else None
                display = node.find("Display").text if node.find(
                    "Display") is not None else None
                low = node.find("Low").text if node.find(
                    "Low") is not None else None
                high = node.find("High").text if node.find(
                    "High") is not None else None
                rows.append({"GHO": indicator, "COUNTRY": country, "SEX": sex, "GHECAUSES": ghecauses,
                             "AGEGROUP": age_group, "YEAR": year, "NUMERIC": numeric,
                             "DISPLAY": display, "LOW": low, "HIGH": high})
    return rows


df_columns = ["GHO", "COUNTRY", "SEX", "GHECAUSES",
              "AGEGROUP", "YEAR", "NUMERIC", "DISPLAY", "LOW", "HIGH"]

all_rows = getAllRows(all_etree, indicator_list)

out_pd = pd.DataFrame(all_rows, columns=df_columns)

out_pd[["NUMERIC"]] = out_pd[["NUMERIC"]].apply(pd.to_numeric)

set_with_dataframe(worksheet, out_pd)
