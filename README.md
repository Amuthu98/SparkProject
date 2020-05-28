![](RackMultipart20200528-4-qtm3mz_html_f8aaa488af7ece8.gif)

# Projet de spark

**Questions préliminaires**

1) La première contrainte que l&#39;architecture de la solution devra prendre en compte est la mémoire générée par les drones. En effet, chaque drone devrait produire 100 Gb de données par jour.

On peut également identifier une deuxième contrainte, le fait que PrestaCop veuille garder chaque message généré par les drones, il va donc falloir des serveurs pour les stocker.

Pour gérer les contraintes ci-dessus, il va falloir mettre en place du streaming. En effet, si les données récupérées ne servent qu&#39;à faire des statistiques, il est inutile de tout stocker, on peut faire directement des analyses en temps réel avec le streaming.

Pour avoir une architecture qui réponde aux différents critères, il faut récupérer les logs générés par les drones (ils deviennent Data Producers), mettre en place un serveur de stockage (HDFS/S3) qui va récupérer les données puis diviser les données en batches et les traiter avec spark streaming (cluster)

2) L&#39;architecture de la solution devra prendre en compte le fait qu&#39;il soit possible qu&#39;un évènement particulier et rare se déclenche (1% des violations observées), cet évènement nécessite une prise de contrôle par un humain pour entrer le bon code de violation.

Pour gérer ces contraintes, il faudra réserver un emplacement de stockage spécifique où seront stockées toutes les données pour lesquelles le drone ne sait pas qualifier la violation, elles seront ensuite traitées par un policier puis renvoyées dans le stream.

3) Prestacop a commis quelques erreurs qui expliquent son premier échec. En effet, ils ont voulu envoyer les données de la police de New York vers les ordinateurs de PrestaCop sans respecter pleinement les contraintes techniques, notamment en termes de mémoire (il est fort probable qu&#39;ils n&#39;aient pas utilisé le streaming).

4) Pour rendre le produit de Prestacop plus rentable, il serait intéressant de rajouter un champ status pour savoir si un policier a modifié le code de violation. En effet, cette information pourrait nous donner des statistiques intéressantes, on pourrait alors savoir à quel point l&#39;intervention humaine est nécessaire pour que la solution soit fonctionnelle, et essayer de réduire cette part au maximum à l&#39;avenir.

**Eléments pour l&#39;architecture**

1) Petit programme python qui va simuler le drone et envoyer les données à notre solution via un stream de données

2) Gérer le message d&#39;alerte du stream

3) Stockage du message dans un serveur de stockage distribué (ex: HDFS/S3)

4) Analyse des données avec du traitement distribué (spark) et répondre à 4 questions pertinentes

5) Chargement du CSV (anciennes infractions) dans notre serveur de stockage distribué. (lignes par lignes)

**Schéma représentant l&#39;architecture de notre solution**

![](RackMultipart20200528-4-qtm3mz_html_c76dbae3aa4f8770.gif) ![](RackMultipart20200528-4-qtm3mz_html_c76dbae3aa4f8770.gif)

STREAM

Script drone

![](RackMultipart20200528-4-qtm3mz_html_57b49e39c7207848.gif) ![](RackMultipart20200528-4-qtm3mz_html_d07e778fab38f15.gif) ![](RackMultipart20200528-4-qtm3mz_html_13f3673276d66015.gif)

Ping

![](RackMultipart20200528-4-qtm3mz_html_4b490e4d60800834.png)

/

![](RackMultipart20200528-4-qtm3mz_html_d558afae2a80130b.png)

![](RackMultipart20200528-4-qtm3mz_html_d1fa7421d1af1150.gif) ![](RackMultipart20200528-4-qtm3mz_html_36495bb73f1a7591.gif) ![](RackMultipart20200528-4-qtm3mz_html_13f3673276d66015.gif)

Alerte

![](RackMultipart20200528-4-qtm3mz_html_e7ea076aa3a2099d.png)

 ![](RackMultipart20200528-4-qtm3mz_html_2b2d540f1daf52f7.png)

![](RackMultipart20200528-4-qtm3mz_html_c31a3207783b81d2.gif)

![](RackMultipart20200528-4-qtm3mz_html_a18f8c3548f219bb.gif) ![](RackMultipart20200528-4-qtm3mz_html_30e10cdff81842ae.gif) ![](RackMultipart20200528-4-qtm3mz_html_87e0eced4d9ca73e.gif) ![](RackMultipart20200528-4-qtm3mz_html_c8153c043fb783ac.gif) ![](RackMultipart20200528-4-qtm3mz_html_a18f8c3548f219bb.gif) ![](RackMultipart20200528-4-qtm3mz_html_7ebea99d7488841f.gif)

Sortie CSV pour ML / Reporting

![](RackMultipart20200528-4-qtm3mz_html_c6babc246042665f.png)

Chargement ligne par ligne

![](RackMultipart20200528-4-qtm3mz_html_c6babc246042665f.png)