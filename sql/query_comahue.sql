SELECT 
  universidad as university,
  carrera as career,
  fecha_de_inscripcion as incription_date,
  LEFT(name,STRPOS (name,' ')) as first_name,
  RIGHT(name,STRPOS (name,' ')+1) as last_name,
  sexo as gender,
  date_part('year',now())-date_part('year',to_date(fecha_nacimiento, 'YYYY-MM-DD')) as age,
  correo_electronico as email

FROM public.flores_comahue

left(Name,strpos (name,' '))