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
