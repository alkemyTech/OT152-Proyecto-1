SELECT universidad,
	   carrera,
	   fecha_de_inscripcion,
	   name,
	   sexo,
	   codigo_postal,
	   direccion,
	   correo_electronico
FROM flores_comahue
WHERE universidad = 'UNIV. NACIONAL DEL COMAHUE'
AND fecha_de_inscripcion BETWEEN '2020-09-1' AND '2021-02-01';