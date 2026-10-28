# Publier le dépôt pour les participants, sans les corrigés

Le README annonce que les corrigés sont publiés à la fin de chaque journée. Méthode simple, avec une branche `participants` :

```bash
# Avant la formation : une branche sans aucun corrigé
git checkout -b participants
git rm -r --cached jour1/corriges jour2/corriges jour3/corriges jour1/python/corriges jour2/python/corriges jour3/python/corriges
printf "jour*/corriges/\njour*/python/corriges/\n" >> .gitignore
git commit -m "Version participants : énoncés seuls"
git push -u origin participants        # lien à donner aux participants

# Fin du jour 1 : publier les corrigés du jour 1 (à partir de main)
git checkout participants
git checkout main -- jour1/corriges jour1/python/corriges
git add -f jour1/corriges jour1/python/corriges && git commit -m "Corrigés du jour 1" && git push
# idem en fin de J2 (jour2/...) et de J3 (jour3/...)
```

Les participants récupèrent les corrigés avec `git pull`. Les scripts `scripts/` et les données ne changent pas d'une branche à l'autre.
