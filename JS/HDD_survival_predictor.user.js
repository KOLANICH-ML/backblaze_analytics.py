// ==UserScript==
// @name				HDD_survival_predictor
// @id					HDD_survival_predictor
// @namespace			HDD_survival_predictor

// @description			Predicts HDD reliability based on BackBlaze Dataset and Cox XGBoost model.
// @version				0.0.1

// @include				https://www.backblaze.com/blog/*
// @include				https://www.amazon.com/*-Desktop-Drive-*
// @include				https://www.amazon.com/*-Desktop-Drive-*
// @include				https://market.yandex.ru/compare/*
// @include				https://market.yandex.ru/product-*zhestkii-disk*
// @include				https://www.wd.com/products/internal-storage/*
// @include				https://www.dns-shop.ru/catalog/*/zhestkie-diski*
// @include				https://www.partsdirect.ru/pc/hdd/*
// @include				https://www.nix.ru/price/price_list.html?section=hdd*
// @include				https://www.regard.ru/catalog/group5000*
// @include				https://www.regard.ru/catalog/group5014*
// @include				https://www.regard.ru/catalog/group5023*
// @include				https://technopoint.ru/product/*/*-zestkij-disk-*/
// @include				https://hard.rozetka.com.ua/hdd/*
// @include				https://server.kh.ua/shop/zhestkie-diski-vnutrennie/*
// @include				https://server.kh.ua/shop/HDD-*.html
// @include				https://komp.1k.by/utility-harddisks/*
// @include				https://www.newegg.com/*-Hard-Drives/SubCategory/ID-*
// @include				https://mdcomputers.in/internal-hdd*
// @include				https://ram.by/parts/hdd.html
// @include				https://price.ua/catalog46.html
// @include				https://www.1a.lv/*_hdd_*
// @include				https://shop.kz/zhestkie-diski-NB/
// @include				https://shop.kz/offer/zhestkiy-disk-hdd-*/
// @include				https://www.xcom-shop.ru/catalog/kompyuternye_komplektyyuschie/hdd_*
// @include				https://www.flipkart.com/*-hard-disk-drive-*
// @include				https://f.ua/shop/zhestkie-diski/*
// @include				https://f.ua/shop/zhestkie-diski/*

// @author				KOLANICH
// @copyright			KOLANICH, 2018
// @license				Unlicense
// @contributionURL		

// @grant				GM.registerMenuCommand
// @grant				GM.getResourceURL
// @grant				GM.setClipboard
// @grant				GM.notification
// @grant				GM.xmlHttpRequest
// @run-at				document-start
// @optimize			1
//
// ==/UserScript==

/*This is free and unencumbered software released into the public domain.
Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.
In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
For more information, please refer to <https://unlicense.org/>*/

"use strict";

function GMFetch(url, data){
	return new Promise( (resolve, reject) => {
		GM.xmlHttpRequest({
			method: "POST",
			url: url,
			data: data,
			headers: {
				"Content-Type": "application/json"
			},
			onload: resolve,
			onerror: reject
		});
	});
}

var rxs={
	"Seagate":'ST\\d+(?:[A-Z]{2}\\d+|AS)(\\s[A-Z]{2})?',
	"Samsung":'H[DAEMS]\\d{3}[A-Z]{2}(?:/p)?|[SM][PV]\\d{4}[A-Z](?:/r)?|(7(PA|PC|TD|TE|WD)|M(P[AC]|TD)|N(LN|TD)|CBQ|C[QR]E|DOE|HPV|YTY)(28G|024|032|E32|56G|64G|256|128|256|480|512)([5F]M[PXU]P|H[MABCD](CD|DR|F[UV]|G[LMR]|H[PQ]|JP|LH))',
	"WD":'(?:WDC )?WD\\d+(?:[A-X][0-29A-Z])?[26A-Z][A-Z]',
	"HGST":'(AL|DT|M[DGCNQ]|PX|THN)\\w{2}[ASPRC]([ABCEX]|[HLMRV])[ABDFQU]\\d{2,3}[0DE]?([ANE]Y|VS?|[BCHPQWDFGR])?|M[BJH]\\w{2}[23]\\d{3}(A[CHT]|B[HJKS]|C[HJ]|F[CD]|N[PC]|RC)',
	"Toshiba":'[WH][UDTECM][HSCEATNP](\\d{2}|5C)\\d{4}[PVDKABCHJLMSG][L795S]([13][68]|F[24]|A[T3]|SA|[AENS]6|SS|[45]2)[0-486][01245]',
}
var modelNameRx=[];
for(var n in rxs){
	modelNameRx.push("(?:"+rxs[n]+")");
}
modelNameRx=new RegExp(modelNameRx.join("|"), "g");

function *rxMatches(rx, s){
	let m, pLx=0;
	while(m = rx.exec(s)){
		yield [m.index-pLx, rx.lastIndex-m.index];
		pLx=rx.lastIndex;
	}
	rx.lastIndex=0;
}

function *walkTree(){
	var treeWalker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
	let n=treeWalker.nextNode();
	let n1;
	do{
		for(let m of rxMatches(modelNameRx, n.textContent)){
			if(m[0])
				n.splitText(m[0]);
				n=treeWalker.nextNode();
			yield n;
			if(m[1])
				n.splitText(m[1]);
				n=treeWalker.nextNode();
		}
		n=treeWalker.nextNode();
	}while(n);
}

/* WebComponents are badly broken in Firefox + GreasyMonkey. Have tried different methods of injection (eval, script, XRay), each one has failed, some because of bugs in FF, some because of bugs in GM, some because of CSP.*/
/*
class HDDModelNameber extends HTMLElement {
	constructor() {
		super();
		let rt = this.attachShadow({mode: 'open'});
		this.nameEl=document.createElement('span');
		this.survivalEl=document.createElement('span');
		rt.appendChild(this.nameEl);
		rt.appendChild(this.survivalEl);
		//observer.disconnect();
	}
	attributeChangedCallback(name, old, nw) {
		switch (name) {
			case "survival":
				this.survivalEl.textContent=nw;
			break;
			default:
				this.nameEl[name]=nw;
			break;
		}
	}
	get survival(){
		return this.getAttribute("survival");
	}
	set survival(val){
		this.setAttribute("survival", val);
		return this.survivalEl.textContent=val;
	}
	static get observedAttributes() {
		return ['survival'];
	}
}
customElements.define(nameberTagName, HDDModelNameber);
*/

function allowPageAccess(obj){
	let unwraping=window.Object();
	unwraping.wrappedJSObject.obj=cloneInto(obj, window, {cloneFunctions: false});
	return unwraping.wrappedJSObject.obj;
}

function getModelsNodes(){
	let models={};

	for(let el of [...walkTree()]){
		console.log(el);
		let mn=el.textContent;
		if(!models[mn]){
			models[mn]=[];
		}
		let nc=document.createElement("span"); // WebComponents are badly broken in Firefox + GreasyMonkey
		el.parentNode.replaceChild(nc, el);
		nc.textContent=el.textContent;
		models[mn].push(nc);
	}
	return models;
}


function SHAPPercent2Color(v){
	return "hsl("+(v<0?150:0)+", "+Math.abs(v)*100+"%, 50%)";
}

function renderPredictions(predictions, models){
	for(let i=0; i<predictions.res.length; ++i){
		let pred=predictions.res[i];
		let expl=predictions.explainations[i];
		let domNodes=models[pred["name"]];
		console.log(domNodes);
		for(let n of domNodes){
			//n.survival=" ("+pred["$fancyTime"]+")";
			n.textContent+=" ("+pred["$fancyTime"]+")"; // WebComponents are badly broken in Firefox + GreasyMonkey
			//n.nameEl.textContent=" ("+pred["$fancyTime"]+")"; // WebComponents are badly broken in Firefox + GreasyMonkey
		}
	}
}

function getMetadataAndSurvival(host="localhost", port=0xbb85){
	let models=getModelsNodes();
	let descriptors = [];
	for(let k in models){
		descriptors.push({"name":k});
	}
	//GM.setClipboard(JSON.stringify(descriptors));
	//GM.notification("processing "+data.length+" drives");
	let uri=`http://${host}:${port}/predict`;
	fetch(uri, { method: 'POST', body: JSON.stringify(descriptors), mode: 'cors', headers: {"Content-Type": "application/json",},}).then( e => {console.log(e); return e;} ).then( r => r.text() ).then( e => {console.log(e); return e;} ).then( r => JSON.parse(r) ).then( e => {return renderPredictions(e, models);} ).catch(console.error.bind(console) );
}

document.addEventListener("DOMContentLoaded", evt => getMetadataAndSurvival(), true);
