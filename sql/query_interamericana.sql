SELECT univiersities, carrera, inscription_dates,
names, sexo, fechas_nacimiento, localidad, direcciones, email
FROM rio_cuarto_interamericana
WHERE univiersities = '-universidad-abierta-interamericana' AND 
TO_DATE(inscription_dates, 'DD/Mon/YY') BETWEEN '2020-09-01' AND '2021-02-01';