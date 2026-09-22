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

from datetime import date, datetime, timedelta
from random import Random
from pyspark.sql import Row

rng = Random(20260908)
spark.sql("CREATE SCHEMA IF NOT EXISTS ontology_demo")

countries = [("Thailand", "Bangkok"), ("Singapore", "Singapore"), ("Malaysia", "Kuala Lumpur"), ("Indonesia", "Jakarta"), ("Vietnam", "Ho Chi Minh City"), ("Philippines", "Manila")]
occupations = ["Engineer", "Teacher", "Analyst", "Designer", "Manager", "Consultant", "Healthcare"]
products_seed = [
    (1, "Smart Fitness Watch", "Fitness", 129.0), (2, "Yoga Mat", "Fitness", 35.0), (3, "Running Shoes", "Fitness", 95.0),
    (4, "Sleep Mask", "Sleep", 18.0), (5, "White Noise Speaker", "Sleep", 55.0), (6, "Weighted Blanket", "Sleep", 110.0),
    (7, "Water Bottle", "Hydration", 24.0), (8, "Electrolyte Mix", "Hydration", 16.0), (9, "Herbal Tea", "Nutrition", 14.0),
    (10, "Protein Powder", "Nutrition", 48.0), (11, "Vitamin Pack", "Nutrition", 30.0), (12, "Healthy Meal Kit", "Nutrition", 42.0),
    (13, "Meditation App", "Mindfulness", 60.0), (14, "Journal", "Mindfulness", 12.0), (15, "Aromatherapy Set", "Mindfulness", 38.0),
    (16, "E-reader", "Learning", 145.0), (17, "Book Subscription", "Learning", 25.0), (18, "Online Course", "Learning", 75.0),
    (19, "Desk Lamp", "Workplace", 45.0), (20, "Standing Desk Mat", "Workplace", 62.0), (21, "Ergonomic Chair", "Workplace", 280.0),
    (22, "Coffee Beans", "Beverage", 20.0), (23, "Decaf Coffee", "Beverage", 21.0), (24, "Smoothie Pack", "Nutrition", 28.0),
    (25, "Resistance Bands", "Fitness", 22.0), (26, "Foam Roller", "Fitness", 32.0), (27, "Cycling Helmet", "Fitness", 68.0),
    (28, "Blue Light Glasses", "Workplace", 40.0), (29, "Air Purifier", "Wellness", 170.0), (30, "Massage Device", "Wellness", 120.0)
]

customers = []
profiles = []
for customer_id in range(1, 251):
    country, city = rng.choice(countries)
    age = rng.randint(18, 70)
    customers.append(Row(
        CustomerID=customer_id,
        CustomerName=f"Customer {customer_id:04d}",
        Age=age,
        Gender=rng.choice(["Female", "Male", "Non-binary"]),
        Country=country,
        City=city,
        Occupation=rng.choice(occupations),
        MembershipTier=rng.choice(["Standard", "Silver", "Gold", "Platinum"]),
        JoinDate=date(2023, 1, 1) + timedelta(days=rng.randint(0, 1200))
    ))
    profiles.append(Row(
        ProfileID=10000 + customer_id,
        CustomerID=customer_id,
        DietType=rng.choice(["Balanced", "Vegetarian", "Vegan", "Low Carb", "High Protein"]),
        SmokingStatus=rng.choice(["Never", "Former", "Current"]),
        AlcoholConsumption=rng.choice(["None", "Occasional", "Moderate"]),
        TargetSleepHours=round(rng.uniform(6.5, 9.0), 1),
        TargetExerciseDays=rng.randint(2, 7),
        WellnessGoal=rng.choice(["Improve sleep", "Reduce stress", "Lose weight", "Build fitness", "Healthy habits"])
    ))

observations = []
observation_id = 1
start_day = date(2026, 8, 1)
for customer_id in range(1, 251):
    baseline_bmi = rng.uniform(19.0, 32.0)
    for day_offset in range(30):
        sleep_hours = round(rng.uniform(5.0, 9.5), 1)
        exercise_minutes = rng.choice([0, 0, 15, 20, 30, 45, 60, 75])
        stress_score = rng.randint(1, 10)
        observations.append(Row(
            ObservationID=observation_id,
            CustomerID=customer_id,
            ObservationDate=start_day + timedelta(days=day_offset),
            SleepHours=sleep_hours,
            Steps=rng.randint(1500, 18000),
            ExerciseMinutes=exercise_minutes,
            WaterIntakeLiters=round(rng.uniform(1.0, 4.0), 1),
            StressScore=stress_score,
            BMI=round(baseline_bmi + rng.uniform(-0.4, 0.4), 1),
            LifeSatisfaction=max(1, min(10, round(5 + (sleep_hours - 7) * 0.8 + exercise_minutes / 45 - stress_score / 5 + rng.uniform(-1, 1))))
        ))
        observation_id += 1

activities = []
activity_id = 1
for customer_id in range(1, 251):
    for _ in range(rng.randint(8, 16)):
        activity_type = rng.choice(["Walking", "Running", "Cycling", "Yoga", "Swimming", "Strength Training", "Meditation"])
        duration = rng.randint(10, 90)
        activities.append(Row(
            ActivityID=activity_id,
            CustomerID=customer_id,
            ActivityDate=start_day + timedelta(days=rng.randint(0, 29)),
            ActivityType=activity_type,
            DurationMinutes=duration,
            CaloriesBurned=round(duration * rng.uniform(3.0, 9.0)),
            Intensity=rng.choice(["Low", "Moderate", "High"]),
            RecordedBy=rng.choice(["Mobile app", "Fitness watch", "Manual entry"])
        ))
        activity_id += 1

products = [Row(ProductID=i, ProductName=n, Category=c, UnitPrice=float(p), IsActive=True) for i, n, c, p in products_seed]
purchases = []
purchase_items = []
purchase_id = 1
purchase_item_id = 1
for customer_id in range(1, 251):
    for _ in range(rng.randint(3, 8)):
        purchased_at = datetime(2026, 8, 1) + timedelta(days=rng.randint(0, 29), hours=rng.randint(8, 21), minutes=rng.randint(0, 59))
        selected_products = rng.sample(products_seed, rng.randint(1, 4))
        total_amount = 0.0
        item_rows = []
        for product_id, product_name, category, unit_price in selected_products:
            quantity = rng.randint(1, 3)
            line_amount = round(unit_price * quantity, 2)
            total_amount += line_amount
            item_rows.append(Row(
                PurchaseItemID=purchase_item_id,
                PurchaseID=purchase_id,
                ProductID=product_id,
                Quantity=quantity,
                UnitPrice=float(unit_price),
                LineAmount=line_amount
            ))
            purchase_item_id += 1
        purchases.append(Row(
            PurchaseID=purchase_id,
            CustomerID=customer_id,
            PurchaseTimestamp=purchased_at,
            Channel=rng.choice(["Mobile app", "Web", "Store"]),
            PaymentMethod=rng.choice(["Credit card", "Debit card", "Digital wallet"]),
            TotalAmount=round(total_amount, 2)
        ))
        purchase_items.extend(item_rows)
        purchase_id += 1

tables = {
    "customers": customers,
    "lifestyle_profiles": profiles,
    "wellness_observations": observations,
    "activities": activities,
    "products": products,
    "purchases": purchases,
    "purchase_items": purchase_items
}

for table_name, table_rows in tables.items():
    spark.createDataFrame(table_rows).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"ontology_demo.{table_name}")

relationship_catalog = [
    Row(RelationshipName="has profile", OriginEntity="Customer", OriginEntityKey="CustomerID", TargetEntity="LifestyleProfile", TargetEntityKey="ProfileID", MappingTable="lifestyle_profiles", OriginMatchColumn="CustomerID", TargetMatchColumn="ProfileID", Cardinality="one-to-one"),
    Row(RelationshipName="has observation", OriginEntity="Customer", OriginEntityKey="CustomerID", TargetEntity="WellnessObservation", TargetEntityKey="ObservationID", MappingTable="wellness_observations", OriginMatchColumn="CustomerID", TargetMatchColumn="ObservationID", Cardinality="one-to-many"),
    Row(RelationshipName="performs activity", OriginEntity="Customer", OriginEntityKey="CustomerID", TargetEntity="Activity", TargetEntityKey="ActivityID", MappingTable="activities", OriginMatchColumn="CustomerID", TargetMatchColumn="ActivityID", Cardinality="one-to-many"),
    Row(RelationshipName="makes purchase", OriginEntity="Customer", OriginEntityKey="CustomerID", TargetEntity="Purchase", TargetEntityKey="PurchaseID", MappingTable="purchases", OriginMatchColumn="CustomerID", TargetMatchColumn="PurchaseID", Cardinality="one-to-many"),
    Row(RelationshipName="contains item", OriginEntity="Purchase", OriginEntityKey="PurchaseID", TargetEntity="PurchaseItem", TargetEntityKey="PurchaseItemID", MappingTable="purchase_items", OriginMatchColumn="PurchaseID", TargetMatchColumn="PurchaseItemID", Cardinality="one-to-many"),
    Row(RelationshipName="references product", OriginEntity="PurchaseItem", OriginEntityKey="PurchaseItemID", TargetEntity="Product", TargetEntityKey="ProductID", MappingTable="purchase_items", OriginMatchColumn="PurchaseItemID", TargetMatchColumn="ProductID", Cardinality="many-to-one")
]
spark.createDataFrame(relationship_catalog).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ontology_demo.relationship_catalog")

demo_questions = [
    Row(Exercise=1, Question="Which Gold or Platinum customers have high average stress and fewer than two activities per week?"),
    Row(Exercise=2, Question="Which wellness product categories are purchased most often by customers whose goal is Improve sleep?"),
    Row(Exercise=3, Question="Compare average life satisfaction for customers who practice Yoga or Meditation against customers who do neither."),
    Row(Exercise=4, Question="Find customers whose sleep improved during the month and show their purchases and preferred activity types."),
    Row(Exercise=5, Question="Trace a purchase from Customer to Purchase to PurchaseItem to Product and explain every relationship used.")
]
spark.createDataFrame(demo_questions).write.format("delta").mode("overwrite").saveAsTable("ontology_demo.demo_questions")

counts = {name: spark.table(f"ontology_demo.{name}").count() for name in tables}
assert counts["customers"] == 250
assert counts["lifestyle_profiles"] == 250
assert counts["wellness_observations"] == 7500
assert spark.sql("SELECT COUNT(*) FROM ontology_demo.lifestyle_profiles p LEFT ANTI JOIN ontology_demo.customers c ON p.CustomerID = c.CustomerID").first()[0] == 0
assert spark.sql("SELECT COUNT(*) FROM ontology_demo.purchase_items i LEFT ANTI JOIN ontology_demo.purchases p ON i.PurchaseID = p.PurchaseID").first()[0] == 0
assert spark.sql("SELECT COUNT(*) FROM ontology_demo.purchase_items i LEFT ANTI JOIN ontology_demo.products p ON i.ProductID = p.ProductID").first()[0] == 0
print("ONTOLOGY_DEMO_READY", counts)

