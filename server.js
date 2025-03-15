
const PORT = 7001;
const express = require('/usr/local/lib/node_modules/express');
const router = express.Router();

const app = express();

router.use((req, res, next)=> {
    console.log('[TRACE]\t', req.headers['x-forwarded-for'], req.url);
    next();
});

router.use('/recce@:lat,:lng', (req, res)=> {
    res.status(501).send('page not implemented yet');
});

router.use('/recce/api/:lat,:lng/:venue?', (req, res)=> {
    res.status(501).send('api not implemented yet');
});

app.use(router);
app.use('/recce', express.static('_public'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get('*', (req, res)=> {
    res.status(404).sendFile('/usr/share/nginx/html/404.html');
});

app.listen(PORT, ()=> {
    console.log(' [INFO]\t', 'listening on', '217.154.54.14:'+PORT);
});
