SELECT universidad, carrerra, fechaiscripccion, nombrre,
sexo, nacimiento, codgoposstal, direccion, eemail
FROM moron_nacional_pampa
WHERE universidad = 'Universidad nacional de la pampa' AND
TO_DATE(fechaiscripccion, 'DD/MM/YYYY') BETWEEN '2020-09-01' AND '2021-02-01';