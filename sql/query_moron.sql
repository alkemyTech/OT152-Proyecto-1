SELECT universidad, carrerra, fechaiscripccion,
nombrre, sexo, codgoposstal, eemail
FROM moron_nacional_pampa
WHERE universidad = 'Universidad de mor¾n' 
AND TO_DATE(fechaiscripccion, 'DD/MM/YYYY') BETWEEN '01/9/2020' AND '01/02/2021';
