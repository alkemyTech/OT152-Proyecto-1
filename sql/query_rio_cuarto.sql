SELECT univiersities, carrera, inscription_dates, 
names, sexo, localidad, email, fechas_nacimiento
FROM rio_cuarto_interamericana
WHERE univiersities = 'Universidad-nacional-de-río-cuarto' 
AND inscription_dates BETWEEN '20/Sep/01' AND '21/Feb/01';

