SELECT universidad, careers,fecha_de_inscripcion,names,sexo,birth_dates,codigo_postal,correos_electronicos 
FROM palermo_tres_de_febrero 
WHERE universidad='universidad_nacional_de_tres_de_febrero' 
AND TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') BETWEEN '01/Sep/20' AND '01/Feb/21'
