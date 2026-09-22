# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8300747-1fcc-478d-9c5f-c821c6c07669",
# META       "default_lakehouse_name": "LifestyleDataLocal",
# META       "default_lakehouse_workspace_id": "aad96fb1-b672-4e7f-a33b-b5180bb59cb2",
# META       "known_lakehouses": [
# META         {
# META           "id": "f8300747-1fcc-478d-9c5f-c821c6c07669"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import Row
from random import Random

rng = Random(20260908)
countries = [("Thailand", "Bangkok"), ("Singapore", "Singapore"), ("Malaysia", "Kuala Lumpur"), ("Indonesia", "Jakarta"), ("Vietnam", "Ho Chi Minh City"), ("Philippines", "Manila")]
occupations = ["Engineer", "Teacher", "Analyst", "Designer", "Manager", "Consultant", "Healthcare"]
education = ["High School", "Bachelor", "Master", "Doctorate"]
diet_types = ["Balanced", "Vegetarian", "Vegan", "Low Carb", "High Protein"]
stress_levels = ["Low", "Medium", "High"]
health_ratings = ["Poor", "Fair", "Good", "Very Good", "Excellent"]

rows = []
for person_id in range(1, 1001):
    country, city = rng.choice(countries)
    age = rng.randint(18, 70)
    exercise = rng.randint(0, 7)
    sleep = round(rng.uniform(4.5, 9.5), 1)
    bmi = round(rng.uniform(17.5, 35.0), 1)
    satisfaction = max(1, min(10, round(5 + (sleep - 7) * 0.7 + exercise * 0.25 + rng.uniform(-2, 2))))
    balance = max(1, min(10, round(6 - rng.uniform(35, 60) / 20 + sleep / 3 + rng.uniform(-1, 1))))
    rows.append(Row(
        PersonID=person_id,
        Name=f"Person {person_id:04d}",
        Age=age,
        Gender=rng.choice(["Female", "Male", "Non-binary"]),
        Country=country,
        City=city,
        Occupation=rng.choice(occupations),
        AnnualIncome=rng.randint(24000, 180000),
        EducationLevel=rng.choice(education),
        MaritalStatus=rng.choice(["Single", "Married", "Divorced", "Widowed"]),
        Children=rng.randint(0, 4),
        ExerciseFrequency=exercise,
        DietType=rng.choice(diet_types),
        SleepHours=sleep,
        StressLevel=rng.choice(stress_levels),
        HobbiesCount=rng.randint(0, 7),
        SocialMediaHours=round(rng.uniform(0, 6), 1),
        ReadingHours=round(rng.uniform(0, 4), 1),
        WorkHoursPerWeek=rng.randint(20, 65),
        CommuteMinutes=rng.randint(0, 120),
        SmokingStatus=rng.choice(["Never", "Former", "Current"]),
        AlcoholConsumption=rng.choice(["None", "Occasional", "Moderate"]),
        CoffeeConsumption=rng.randint(0, 6),
        WaterIntakeLiters=round(rng.uniform(0.8, 4.0), 1),
        BMI=bmi,
        HealthRating=rng.choice(health_ratings),
        LifeSatisfaction=satisfaction,
        WorkLifeBalance=balance
    ))

df = spark.createDataFrame(rows)
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.lifestyle_data")
actual_count = spark.table("dbo.lifestyle_data").count()
assert actual_count == 1000, f"Expected 1000 rows, found {actual_count}"
print(f"CREATED dbo.lifestyle_data with {actual_count} rows")

