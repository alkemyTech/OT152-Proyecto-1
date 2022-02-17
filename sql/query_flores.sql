SELECT universidad,carrera,fecha_de_inscripcion,name,sexo,fecha_nacimiento,codigo_postal,direccion,correo_electronico 
FROM public.flores_comahue
WHERE universidad='UNIVERSIDAD DE FLORES'
AND fecha_de_inscripcion BETWEEN '2020-09-01' AND '2021-02-01'