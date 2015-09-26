from django.db import models


class Club(models.Model):
    name = models.CharField(max_length=255)
