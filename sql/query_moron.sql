SELECT universidad, carrerra, fechaiscripccion,
nombrre, sexo, codgoposstal, eemail, nacimiento
FROM moron_nacional_pampa
WHERE universidad = 'Universidad de morón' 
AND TO_DATE(fechaiscripccion, 'DD/MM/YYYY') BETWEEN '01/9/2020' AND '01/02/2021';
