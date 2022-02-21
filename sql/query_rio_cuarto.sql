SELECT univiersities, carrera, inscription_dates, 
names, sexo, localidad, email 
FROM rio_cuarto_interamericana
WHERE univiersities = 'Universidad-nacional-de-rÝo-cuarto' 
AND inscription_dates BETWEEN '20/Sep/01' AND '21/Feb/01';

