SELECT 
  universidad as university,
  carrera as career,
  fecha_de_inscripcion as incription_date,
  LEFT(name,STRPOS (name,' ')) as first_name,
  RIGHT(name,STRPOS (name,' ')+1) as last_name,
  sexo as gender,
  correo_electronico as email

FROM public.flores_comahue

left(Name,strpos (name,' '))