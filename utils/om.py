import pandas as pd

def om_to_dataframe(data: dict, country: str = None) -> pd.DataFrame:
    """
    Convierte la respuesta de Open-Meteo en un DataFrame plano.
    """
    daily = data["daily"]

    df = pd.DataFrame({
        "date": daily["time"],
        #"temperature_2m_max": daily["temperature_2m_max"],
        #"temperature_2m_min": daily["temperature_2m_min"],
        "precipitation_sum": daily["precipitation_sum"],
        "et0_fao_evapotranspiration": daily["et0_fao_evapotranspiration"],
    })

    # revisar
    if country:
        df.insert(0, "country", country)

    return df