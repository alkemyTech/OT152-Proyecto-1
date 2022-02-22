SELECT  universidad AS university , careers AS career, fecha_de_inscripcion AS inscription_date,
        names ,sexo AS gender,birth_dates,codigo_postal AS postal_code,correos_electronicos AS email
FROM palermo_tres_de_febrero 
WHERE universidad='universidad_nacional_de_tres_de_febrero' 
AND TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') BETWEEN '01/Sep/20' AND '01/Feb/21'
