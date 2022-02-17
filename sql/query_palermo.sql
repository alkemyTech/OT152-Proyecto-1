SELECT universidad,careers,fecha_de_inscripcion,names,sexo,birth_dates,codigo_postal,direcciones,correos_electronicos
FROM public.palermo_tres_de_febrero
where universidad='_universidad_de_palermo'
and TO_DATE(fecha_de_inscripcion,'DD/Mon/YY') between '01/Sep/20' and '01/Feb/21'