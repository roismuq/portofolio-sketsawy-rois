/*Data Exploration COVID-19 (data source=ourworlddata.org accessed in 22/01/23)
Query run in PostgreSQL
Skills used: create table, import data, CTE, join table, temp table, 
convert data type, aggregate function, create view
======
sketsawy-rois*/


/*CreatTableVaccination*/
CREATE TABLE CovidVaccination(
	iso_code VARCHAR(100),
	continent VARCHAR(100),
	location VARCHAR(100),
	date date,
	total_tests float,	
	new_tests float, 
	total_tests_per_thousand float,
	new_tests_per_thousand float,
	new_tests_smoothed float,
	new_tests_smoothed_per_thousand float,
	positive_rate float,
	tests_per_case float,
	tests_units VARCHAR(100),
	total_vaccinations float,
	people_vaccinated float,
	people_fully_vaccinated float,
	total_boosters float,
	new_vaccinations float,
	new_vaccinations_smoothed float,
	total_vaccinations_per_hundred float,
	people_vaccinated_per_hundred float,
	people_fully_vaccinated_per_hundred float,
	total_boosters_per_hundred float,
	new_vaccinations_smoothed_per_million float,
	new_people_vaccinated_smoothed float,
	new_people_vaccinated_smoothed_per_hundred float,
	stringency_index float,
	population_density float,
	median_age float,
	aged_65_older float,
	aged_70_older float,
	gdp_per_capita float,
	extreme_poverty float,
	cardiovasc_death_rate float,
	diabetes_prevalence float,
	female_smokers float,
	male_smokers float,
	handwashing_facilities float,
	hospital_beds_per_thousand	float,
	life_expectancy float,
	human_development_index	float,
	population	float,
	excess_mortality_cumulative_absolute float,
	excess_mortality_cumulative	float,
	excess_mortality float,
	excess_mortality_cumulative_per_million float
);

/*import data*/
COPY CovidVaccination
FROM '/Users/rmfatawy/Downloads/CovidVaccination3.csv'
DELIMITER ';'
CSV HEADER;


/*delete table*/
drop table CovidVaccination;

/*select table*/
SELECT * FROM CovidVaccination;

/*createtable covid death*/
CREATE TABLE CovidDeaths(
	iso_code VARCHAR(100),
	continent VARCHAR(100),
	location VARCHAR(100),
	date date,
	population float,
	total_cases	float,
	new_cases float,
	new_cases_smoothed float,
	total_deaths float,
	new_deaths float,
	new_deaths_smoothed	float,
	total_cases_per_million	float,
	new_cases_per_million float,
	new_cases_smoothed_per_million float,
	total_deaths_per_million float,
	new_deaths_per_million float,
	new_deaths_smoothed_per_million float,
	reproduction_rate float,
	icu_patients float,
	icu_patients_per_million float,	
	hosp_patients float,
	hosp_patients_per_million float,
	weekly_icu_admissions float,
	weekly_icu_admissions_per_million float,
	weekly_hosp_admissions float,
	weekly_hosp_admissions_per_million float)
	
/*select table*/
SELECT * FROM CovidDeaths;

/*delete table*/
drop table CovidDeaths;


/*import data*/
COPY CovidDeaths
FROM '/Users/rmfatawy/Downloads/CovidDeaths.csv'
DELIMITER ';'
CSV HEADER;


/*data exploration*/
SELECT * FROM CovidDeaths
WHERE continent is not null
ORDER by 3,4;


SELECT * FROM CovidVaccination
ORDER by 3,4;

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM CovidDeaths
ORDER by 1,2;

/*total cases vs death*/
/*death rate in each country*/
SELECT location, date, total_cases, total_deaths, 
	(total_deaths/NULLIF (total_cases,0))*100 as Deaths_percentage
FROM CovidDeaths
WHERE location like 'Indonesia'
and continent is not null
ORDER by 1,2;

/*looking at Countries with Highest Infection Rate compared to Population*/
SELECT location, population, MAX(total_cases) as HighestInfectionCount, 
MAX((total_cases/population))*100 as PercentPopulationInfected
FROM CovidDeaths
GROUP by location, population
ORDER by PercentPopulationInfected desc;


/*looking at Countries with Highest Death Count per Population*/
SELECT location, MAX(cast(total_deaths as int)) as TotalDeathCount
FROM CovidDeaths
WHERE continent is not null
and total_deaths is not null
GROUP by location
ORDER by TotalDeathCount desc;


/*break down by continent*/
/*showing continents with highest death count per population*/
SELECT continent, MAX(cast(total_deaths as int)) as TotalDeathCount
FROM CovidDeaths
WHERE continent is not null
GROUP by continent
ORDER by TotalDeathCount desc;

/*break down by global*/
SELECT sum(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, 
SUM(cast(new_deaths as int))/SUM(new_cases)*100 as DeathPercentage
FROM CovidDeaths
--WHERE location like 'Indonesia'
WHERE continent is not null
--GROUP by date
ORDER by 1,2;


/*join table*/
SELECT * 
FROM CovidDeaths cde
	Join CovidVaccination cva
	On cde.location = cva.location
	and cde.date = cva.date;
	
/*looking at Total Population vs Vaccination*/
SELECT cde.continent, cde.location, cde.date, 
cde.population, cva.new_vaccinations,
SUM(CAST(cva.new_vaccinations as int)) 
OVER (Partition by cde.location Order by cde.location, cde.date) as Accumulated_People_Vaccinated
FROM CovidDeaths cde
	Join CovidVaccination cva
	On cde.location = cva.location
	and cde.date = cva.date
WHERE cde.continent is not null
order by 2,3;

/*CTE*/
With PopvsVac (continent, location, date, population, new_accination, Accumulated_People_Vaccinated)
as
(
SELECT cde.continent, cde.location, cde.date, 
cde.population, cva.new_vaccinations,
SUM(CAST(cva.new_vaccinations as int)) 
OVER (Partition by cde.location Order by cde.location, cde.date) as Accumulated_People_Vaccinated
FROM CovidDeaths cde
	Join CovidVaccination cva
	On cde.location = cva.location
	and cde.date = cva.date
WHERE cde.continent is not null
--order by 2,3;
)
SELECT *,(Accumulated_People_Vaccinated/Population)*100
FROM PopvsVac

/*TEMPORARY TABLE*/
CREATE TABLE if not exists PercentPopulationVaccinated(
	continent varchar(250),
	location varchar(250),
	date date,
	population numeric,
	new_vaccination numeric,
	Accumulated_People_Vaccinated numeric	
);

INSERT INTO PercentPopulationVaccinated
SELECT cde.continent, cde.location, cde.date, 
cde.population, cva.new_vaccinations,
SUM(CAST(cva.new_vaccinations as int)) 
OVER (Partition by cde.location Order by cde.location, cde.date) as Accumulated_People_Vaccinated
FROM CovidDeaths cde
	Join CovidVaccination cva
	On cde.location = cva.location
	and cde.date = cva.date
WHERE cde.continent is not null;
--order by 2,3;

SELECT *, (Accumulated_People_Vaccinated/Population)*100
FROM PercentPopulationVaccinated;


/*create view*/
CREATE VIEW PercentPopulationVaccinatedView as
SELECT cde.continent, cde.location, cde.date, 
cde.population, cva.new_vaccinations,
SUM(CAST(cva.new_vaccinations as int)) 
OVER (Partition by cde.location Order by cde.location, cde.date) as Accumulated_People_Vaccinated
FROM CovidDeaths cde
	Join CovidVaccination cva
	On cde.location = cva.location
	and cde.date = cva.date
WHERE cde.continent is not null;
--order by 2,3;

SELECT *
FROM PercentPopulationVaccinated;
