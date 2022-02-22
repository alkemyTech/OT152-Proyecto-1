SELECT 
  universidad as university,
  carrera as career,
  fecha_de_inscripcion as inscription_date,
  LEFT(nombre,STRPOS (nombre,'_')) as first_name,
  RIGHT(nombre,STRPOS (nombre,'_')+1) as last_name,
  sexo as gender,
  date_part('year',now())-date_part('year',to_date(fecha_nacimiento, 'DD-Mon-YY')) as age,
  email
  
FROM public.salvador_villa_maria
WHERE universidad= 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA'
AND TO_DATE(fecha_de_inscripcion,'DD-Mon-YY') BETWEEN '01/09/2020' AND '01/02/2021'