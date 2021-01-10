def month_string(month):
    sw = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December"
    }

    return sw.get(month)

def num_days_a_month(month):
    if month in [1,3,5,7,8,10,12]:
        return 31
    elif month == 2:
        return 29
    else:
        return 30


def get_correct_columns(smoothed, totals, relative):
    '''
    total_cases,
    new_cases,
    new_cases_smoothed,
    total_deaths,
    new_deaths,
    new_deaths_smoothed,
    total_cases_per_million,
    new_cases_per_million,
    new_cases_smoothed_per_million,
    total_deaths_per_million,
    new_deaths_per_million,
    new_deaths_smoothed_per_million
    '''
    if totals: 
        if relative:
            cases = 'total_cases_per_million'
            cases_text = 'Total Cases per Million'
            deaths = 'total_deaths_per_million'
            deaths_text = 'Total Deaths per Million'
        else: 
            cases = 'total_cases'
            cases_text = 'Total Cases'
            deaths = 'total_deaths'
            deaths_text = 'Total Deaths'
    else:
        if smoothed:
            if relative:
                cases = 'new_cases_smoothed_per_million'
                cases_text = 'New Cases Smoothed per Million'
                deaths = 'new_deaths_smoothed_per_million'
                deaths_text = 'New Deaths Smoothed per Million'
            else:
                cases = 'new_cases_smoothed'
                cases_text = 'New Cases Smoothed'
                deaths = 'new_deaths_smoothed'
                deaths_text = 'New Deaths Smoothed'
        else:
            if relative:
                cases = 'new_cases_per_million'
                cases_text = 'New Cases per Million'
                deaths = 'new_deaths_per_million'
                deaths_text = 'New Deaths per Million'
            else:
                cases = 'new_cases'
                cases_text = 'New Cases'
                deaths = 'new_deaths'
                deaths_text = 'New Deaths'
    return cases, deaths, cases_text, deaths_text



def get_column_natural_name(column):
    sw = {
        'stringency_index': 'Stringency index',
        'population': 'Population',
        'population_density':'Population density',
        'median_age': 'Median age',
        'aged_65_older': 'Population older than 65',
        'aged_70_older': 'Population older than 70',
        'gdp_per_capita': 'Gdp per capita',
        'extreme_poverty': 'Extreme poverty ',
        'cardiovasc_death_rate': 'Cardiovacular death rate',
        'diabetes_prevalence': 'Diabetes prevalence',
        'female_smokers': 'Female smokers',
        'male_smokers': 'Male smokers', 
        'handwashing_facilities': 'Handwashing facilities',
        'hospital_beds_per_thousand':'Hospital beds per thousand',
        'life_expectancy':'Life expectancy',
        'human_development_index':'Development index',
    }
    return sw.get(column)

def get_correct_names_aggregate(avg):
    if avg:
        aggregate = 'avg'
        file_text = 'avg'
        text = 'Average daily'
    else:
        aggregate = 'sum'
        file_text = 'total'
        text = 'Total'
    return aggregate, file_text, text

