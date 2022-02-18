SELECT universidad, carrerra, fechaiscripccion,
nombrre, sexo, codgoposstal, eemail
FROM moron_nacional_pampa
WHERE universidad = 'Universidad De Morón' AND
DATE(fechaiscripccion) BETWEEN '01/9/2020' AND '01/02/2021';