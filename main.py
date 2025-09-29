# from pyspark.sql import SparkSession, window
# from pyspark.sql.functions import *

# # Créer une session Spark
# spark = SparkSession.builder.appName("TestPySpark").getOrCreate()

# # Afficher les infos de la session
# print("Session Spark créée avec succès !")
# df_matches = spark.read.format('csv').options(header = 'True').load('data/football_matches.csv')
# df_matches.limit(20).show()

# df_matches = df_matches.selectExpr(
#     "*",
#     "`FTHG` AS `HomeTeamGoals`",
#     "`FTAG` AS `AwayTeamGoals`",
#     "`FTR` AS `FinalResult`"
# )

# df_matches.limit(20).show()
# df_matches = df_matches.drop("FTHG","FTAG","FTR")
# df_matches.limit(20).show()

# # create binary columns for wins losses and drows
# df_matches = df_matches.withColumn("HomeTeamWin", when(col("FinalResult") == "H", 1 ).otherwise(0)).withColumn("AwayTeamWin", when(col("FinalResult") == "A", 1 ).otherwise(0)).withColumn("GameTie", when(col("FinalResult") == "D", 1 ).otherwise(0))
# df_matches.limit(20).show()

# df_bundesliga = df_matches.filter((col("Div") == "D1") & (col("Season") >= 2000) & (col("Season") <= 2015))
# df_bundesliga.limit(20).show()


# # create a dataframe contain a statisstics on home matches
# df_home_matches = df_bundesliga.groupby('Season', 'HomeTeam') \
#         .agg(sum('HomeTeamWin').alias('TotalHomeWin'),
#              sum('AwayTeamWin').alias('TotalHomeLoss'),
#              sum('GameTie').alias('TotalHomeTie'),
#              sum('HomeTeamGoals').alias('HomeScoredGoals'),
#              sum('AwayTeamGoals').alias('HomeAgainstGoals')) \
#         .withColumnRenamed('HomeTeam', 'Team')
# df_home_matches = df_home_matches #.filter((col("Team") == "Hamburg"))    
# df_home_matches.show()


# # create a dataframe contain a statisstics on Away matches
# df_Away_matches = df_bundesliga.groupby('Season', 'AwayTeam') \
#         .agg(sum('AwayTeamWin').alias('TotalAwayWin'),
#              sum('HomeTeamWin').alias('TotalAwayLoss'),
#              sum('GameTie').alias('TotalAwayTie'),
#              sum('AwayTeamGoals').alias('AwayScoredGoals'),
#              sum('HomeTeamGoals').alias('AwayAgainstGoals')) \
#         .withColumnRenamed('AwayTeam', 'Team')
   
# df_Away_matches.show()

# # join home and away data 
# df_merged = df_home_matches.join(df_Away_matches,["Season", "Team"], "inner")
# df_merged.show()
# # df_pandas = df_merged.toPandas()
# # df_pandas.to_excel("df_matches.xlsx", index=False, engine='openpyxl')

# #create new columns for total scores and results
# df_totals = df_merged.withColumn("GaolsScored", col("HomeScoredGoals") + col("AwayScoredGoals")).withColumn("GaolsAgainst", col("HomeAgainstGoals") + col("AwayAgainstGoals")).withColumn("Win", col("TotalHomeWin") + col("TotalAwayWin")).withColumn("Loss", col("TotalHomeLoss") + col("TotalAwayLoss")).withColumn("Tie", col("TotalHomeTie") + col("TotalAwayTie"))

# df_totals.show()

# # drop unnecesarly columns
# cols_to_drop = ["TotalHomeWin","TotalHomeLoss","TotalHomeTie","HomeScoredGoals","HomeAgainstGoals","TotalAwayWin","TotalAwayLoss","TotalAwayTie","AwayScoredGoals","AwayAgainstGoals"]

# df_cleaned = df_totals.drop(*cols_to_drop)
# df_cleaned.show()

# # create additional columns
# df_processed = df_cleaned.withColumn("GaolDiferentials", col("GaolsScored") - col("GaolsAgainst")).withColumn("WinPersentage", round( 100 * col("Win") / (col("Win") + col("Loss") + col("Tie")), 2) )

# df_processed.show()

# # Arrêter Spark
# # spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, round

# Créer une session Spark
spark = SparkSession.builder.appName("AnalyseFootball").getOrCreate()

# Afficher les infos de la session
print("Session Spark créée avec succès !")

# Charger les données
df_matchs = spark.read.format('csv').options(header='True').load('data/football_matches.csv')
df_matchs.limit(20).show()

# Renommer les colonnes principales
df_matchs = df_matchs.selectExpr(
    "*",
    "`FTHG` AS `ButsEquipeDomicile`",
    "`FTAG` AS `ButsEquipeExterieur`",
    "`FTR` AS `ResultatFinal`"
)

df_matchs = df_matchs.drop("FTHG", "FTAG", "FTR")
df_matchs.limit(20).show()

# Créer des colonnes binaires pour Victoire, Défaite, Match nul
df_matchs = (
    df_matchs
    .withColumn("VictoireDomicile", when(col("ResultatFinal") == "H", 1).otherwise(0))
    .withColumn("VictoireExterieur", when(col("ResultatFinal") == "A", 1).otherwise(0))
    .withColumn("MatchNul", when(col("ResultatFinal") == "D", 1).otherwise(0))
)
df_matchs.limit(20).show()

# Filtrer uniquement Bundesliga (D1) entre 2000 et 2015
df_bundesliga = df_matchs.filter(
    (col("Div") == "D1") & (col("Season") >= 2000) & (col("Season") <= 2015)
)
df_bundesliga.limit(20).show()

# Statistiques des matchs à domicile
df_domicile = df_bundesliga.groupby('Season', 'HomeTeam') \
        .agg(sum('VictoireDomicile').alias('TotalVictoiresDomicile'),
             sum('VictoireExterieur').alias('TotalDefaitesDomicile'),
             sum('MatchNul').alias('TotalMatchsNulsDomicile'),
             sum('ButsEquipeDomicile').alias('ButsMarquesDomicile'),
             sum('ButsEquipeExterieur').alias('ButsEncaissesDomicile')) \
        .withColumnRenamed('HomeTeam', 'Equipe')

df_domicile.show()

# Statistiques des matchs à l’extérieur
df_exterieur = df_bundesliga.groupby('Season', 'AwayTeam') \
        .agg(sum('VictoireExterieur').alias('TotalVictoiresExterieur'),
             sum('VictoireDomicile').alias('TotalDefaitesExterieur'),
             sum('MatchNul').alias('TotalMatchsNulsExterieur'),
             sum('ButsEquipeExterieur').alias('ButsMarquesExterieur'),
             sum('ButsEquipeDomicile').alias('ButsEncaissesExterieur')) \
        .withColumnRenamed('AwayTeam', 'Equipe')

df_exterieur.show()

# Fusion domicile et extérieur
df_fusion = df_domicile.join(df_exterieur, ["Season", "Equipe"], "inner")
df_fusion.show()

# Statistiques globales
df_totaux = (
    df_fusion
    .withColumn("ButsMarques", col("ButsMarquesDomicile") + col("ButsMarquesExterieur"))
    .withColumn("ButsEncaisses", col("ButsEncaissesDomicile") + col("ButsEncaissesExterieur"))
    .withColumn("Victoires", col("TotalVictoiresDomicile") + col("TotalVictoiresExterieur"))
    .withColumn("Defaites", col("TotalDefaitesDomicile") + col("TotalDefaitesExterieur"))
    .withColumn("MatchsNuls", col("TotalMatchsNulsDomicile") + col("TotalMatchsNulsExterieur"))
)

df_totaux.show()

# Supprimer les colonnes inutiles
colonnes_a_supprimer = [
    "TotalVictoiresDomicile","TotalDefaitesDomicile","TotalMatchsNulsDomicile",
    "ButsMarquesDomicile","ButsEncaissesDomicile",
    "TotalVictoiresExterieur","TotalDefaitesExterieur","TotalMatchsNulsExterieur",
    "ButsMarquesExterieur","ButsEncaissesExterieur"
]
df_nettoye = df_totaux.drop(*colonnes_a_supprimer)
df_nettoye.show()

# Ajouter de nouveaux indicateurs
df_final = (
    df_nettoye
    .withColumn("DifferenceButs", col("ButsMarques") - col("ButsEncaisses"))
    .withColumn("PourcentageVictoires", round(100 * col("Victoires") / (col("Victoires") + col("Defaites") + col("MatchsNuls")), 2))
)

df_final.show()

# Arrêter Spark
# spark.stop()




