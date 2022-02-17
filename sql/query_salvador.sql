SELECT *
FROM salvador_villa_maria
WHERE universidad = 'UNIVERSIDAD_DEL_SALVADOR'
AND TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') between '01/Sep/20' and '01/Feb/21';