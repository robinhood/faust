from django.db import models
from django.utils.translation import ugettext_lazy as _


class Account(models.Model):
    name = models.CharField(_('name'), max_length=100)
    score = models.DecimalField(_('score'), default=0.0,
                                max_digits=1000, decimal_places=1000)
    active = models.BooleanField(_('active'), default=True)

    class Meta:
        verbose_name = _('account')
        verbose_name_plural = _('accounts')
