SELECT 
  universidad as university,
  carrera as career,
  fecha_de_inscripcion as incription_date,
  LEFT(name,STRPOS (name,' ')) as first_name,
  RIGHT(name,STRPOS (name,' ')+1) as last_name,
  sexo as gender,
  date_part('year',now())-date_part('year',to_date(fecha_nacimiento, 'YYYY-MM-DD')) as age,
  codigo_postal as postal_code,
  correo_electronico as email
FROM public.flores_comahue
AND TO_DATE(inscription_dates,'DD-MM-YYYY') BETWEEN '01/09/2020' AND '01/02/2021';