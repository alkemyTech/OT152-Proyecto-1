select universidad,carrera,fecha_de_inscripcion,name,sexo,fecha_nacimiento,codigo_postal,direccion,correo_electronico 
from public.flores_comahue
where universidad='UNIVERSIDAD DE FLORES'
and fecha_de_inscripcion between '2020-09-01' and '2021-02-01'