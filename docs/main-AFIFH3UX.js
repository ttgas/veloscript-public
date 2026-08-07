var NE=Object.defineProperty,PE=Object.defineProperties;var OE=Object.getOwnPropertyDescriptors;var iy=Object.getOwnPropertySymbols;var LE=Object.prototype.hasOwnProperty,FE=Object.prototype.propertyIsEnumerable;var ry=(n,e,t)=>e in n?NE(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,pt=(n,e)=>{for(var t in e||={})LE.call(e,t)&&ry(n,t,e[t]);if(iy)for(var t of iy(e))FE.call(e,t)&&ry(n,t,e[t]);return n},St=(n,e)=>PE(n,OE(e));var jt=(n,e,t)=>new Promise((i,r)=>{var s=l=>{try{a(t.next(l))}catch(c){r(c)}},o=l=>{try{a(t.throw(l))}catch(c){r(c)}},a=l=>l.done?i(l.value):Promise.resolve(l.value).then(s,o);a((t=t.apply(n,e)).next())});function wd(n,e){return Object.is(n,e)}var Nt=null,nl=!1,Sd=1,Fn=Symbol("SIGNAL");function $e(n){let e=Nt;return Nt=n,e}function Cd(){return Nt}var Oo={version:0,lastCleanEpoch:0,dirty:!1,producerNode:void 0,producerLastReadVersion:void 0,producerIndexOfThis:void 0,nextProducerIndex:0,liveConsumerNode:void 0,liveConsumerIndexOfThis:void 0,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function rl(n){if(nl)throw new Error("");if(Nt===null)return;Nt.consumerOnSignalRead(n);let e=Nt.nextProducerIndex++;if(al(Nt),e<Nt.producerNode.length&&Nt.producerNode[e]!==n&&Po(Nt)){let t=Nt.producerNode[e];ol(t,Nt.producerIndexOfThis[e])}Nt.producerNode[e]!==n&&(Nt.producerNode[e]=n,Nt.producerIndexOfThis[e]=Po(Nt)?oy(n,Nt,e):0),Nt.producerLastReadVersion[e]=n.version}function sy(){Sd++}function Td(n){if(!(Po(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Sd)){if(!n.producerMustRecompute(n)&&!Rd(n)){bd(n);return}n.producerRecomputeValue(n),bd(n)}}function Dd(n){if(n.liveConsumerNode===void 0)return;let e=nl;nl=!0;try{for(let t of n.liveConsumerNode)t.dirty||kE(t)}finally{nl=e}}function Ad(){return Nt?.consumerAllowSignalWrites!==!1}function kE(n){n.dirty=!0,Dd(n),n.consumerMarkedDirty?.(n)}function bd(n){n.dirty=!1,n.lastCleanEpoch=Sd}function sl(n){return n&&(n.nextProducerIndex=0),$e(n)}function Id(n,e){if($e(e),!(!n||n.producerNode===void 0||n.producerIndexOfThis===void 0||n.producerLastReadVersion===void 0)){if(Po(n))for(let t=n.nextProducerIndex;t<n.producerNode.length;t++)ol(n.producerNode[t],n.producerIndexOfThis[t]);for(;n.producerNode.length>n.nextProducerIndex;)n.producerNode.pop(),n.producerLastReadVersion.pop(),n.producerIndexOfThis.pop()}}function Rd(n){al(n);for(let e=0;e<n.producerNode.length;e++){let t=n.producerNode[e],i=n.producerLastReadVersion[e];if(i!==t.version||(Td(t),i!==t.version))return!0}return!1}function Nd(n){if(al(n),Po(n))for(let e=0;e<n.producerNode.length;e++)ol(n.producerNode[e],n.producerIndexOfThis[e]);n.producerNode.length=n.producerLastReadVersion.length=n.producerIndexOfThis.length=0,n.liveConsumerNode&&(n.liveConsumerNode.length=n.liveConsumerIndexOfThis.length=0)}function oy(n,e,t){if(ay(n),n.liveConsumerNode.length===0&&ly(n))for(let i=0;i<n.producerNode.length;i++)n.producerIndexOfThis[i]=oy(n.producerNode[i],n,i);return n.liveConsumerIndexOfThis.push(t),n.liveConsumerNode.push(e)-1}function ol(n,e){if(ay(n),n.liveConsumerNode.length===1&&ly(n))for(let i=0;i<n.producerNode.length;i++)ol(n.producerNode[i],n.producerIndexOfThis[i]);let t=n.liveConsumerNode.length-1;if(n.liveConsumerNode[e]=n.liveConsumerNode[t],n.liveConsumerIndexOfThis[e]=n.liveConsumerIndexOfThis[t],n.liveConsumerNode.length--,n.liveConsumerIndexOfThis.length--,e<n.liveConsumerNode.length){let i=n.liveConsumerIndexOfThis[e],r=n.liveConsumerNode[e];al(r),r.producerIndexOfThis[i]=e}}function Po(n){return n.consumerIsAlwaysLive||(n?.liveConsumerNode?.length??0)>0}function al(n){n.producerNode??=[],n.producerIndexOfThis??=[],n.producerLastReadVersion??=[]}function ay(n){n.liveConsumerNode??=[],n.liveConsumerIndexOfThis??=[]}function ly(n){return n.producerNode!==void 0}function Pd(n,e){let t=Object.create(UE);t.computation=n,e!==void 0&&(t.equal=e);let i=()=>{if(Td(t),rl(t),t.value===il)throw t.error;return t.value};return i[Fn]=t,i}var Md=Symbol("UNSET"),Ed=Symbol("COMPUTING"),il=Symbol("ERRORED"),UE=St(pt({},Oo),{value:Md,dirty:!0,error:null,equal:wd,kind:"computed",producerMustRecompute(n){return n.value===Md||n.value===Ed},producerRecomputeValue(n){if(n.value===Ed)throw new Error("Detected cycle in computations.");let e=n.value;n.value=Ed;let t=sl(n),i,r=!1;try{i=n.computation(),$e(null),r=e!==Md&&e!==il&&i!==il&&n.equal(e,i)}catch(s){i=il,n.error=s}finally{Id(n,t)}if(r){n.value=e;return}n.value=i,n.version++}});function VE(){throw new Error}var cy=VE;function uy(n){cy(n)}function Od(n){cy=n}var BE=null;function Ld(n,e){let t=Object.create(kd);t.value=n,e!==void 0&&(t.equal=e);let i=()=>(rl(t),t.value);return i[Fn]=t,i}function ll(n,e){Ad()||uy(n),n.equal(n.value,e)||(n.value=e,HE(n))}function Fd(n,e){Ad()||uy(n),ll(n,e(n.value))}var kd=St(pt({},Oo),{equal:wd,value:void 0,kind:"signal"});function HE(n){n.version++,sy(),Dd(n),BE?.()}function Ud(n){let e=$e(null);try{return n()}finally{$e(e)}}var Vd;function Lo(){return Vd}function _i(n){let e=Vd;return Vd=n,e}var cl=Symbol("NotFound");function st(n){return typeof n=="function"}function ul(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var dl=ul(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Fo(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var Jt=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(st(i))try{i()}catch(s){e=s instanceof dl?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{dy(s)}catch(o){e=e??[],o instanceof dl?e=[...e,...o.errors]:e.push(o)}}if(e)throw new dl(e)}}add(e){var t;if(e&&e!==this)if(this.closed)dy(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Fo(t,e)}remove(e){let{_finalizers:t}=this;t&&Fo(t,e),e instanceof n&&e._removeParent(this)}};Jt.EMPTY=(()=>{let n=new Jt;return n.closed=!0,n})();var Bd=Jt.EMPTY;function fl(n){return n instanceof Jt||n&&"closed"in n&&st(n.remove)&&st(n.add)&&st(n.unsubscribe)}function dy(n){st(n)?n():n.unsubscribe()}var kn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var as={setTimeout(n,e,...t){let{delegate:i}=as;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=as;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function hl(n){as.setTimeout(()=>{let{onUnhandledError:e}=kn;if(e)e(n);else throw n})}function Hd(){}var fy=zd("C",void 0,void 0);function hy(n){return zd("E",void 0,n)}function py(n){return zd("N",n,void 0)}function zd(n,e,t){return{kind:n,value:e,error:t}}var fr=null;function ls(n){if(kn.useDeprecatedSynchronousErrorHandling){let e=!fr;if(e&&(fr={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=fr;if(fr=null,t)throw i}}else n()}function my(n){kn.useDeprecatedSynchronousErrorHandling&&fr&&(fr.errorThrown=!0,fr.error=n)}var hr=class extends Jt{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,fl(e)&&e.add(this)):this.destination=qE}static create(e,t,i){return new cs(e,t,i)}next(e){this.isStopped?Wd(py(e),this):this._next(e)}error(e){this.isStopped?Wd(hy(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Wd(fy,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},jE=Function.prototype.bind;function Gd(n,e){return jE.call(n,e)}var jd=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){pl(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){pl(i)}else pl(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){pl(t)}}},cs=class extends hr{constructor(e,t,i){super();let r;if(st(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&kn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&Gd(e.next,s),error:e.error&&Gd(e.error,s),complete:e.complete&&Gd(e.complete,s)}):r=e}this.destination=new jd(r)}};function pl(n){kn.useDeprecatedSynchronousErrorHandling?my(n):hl(n)}function $E(n){throw n}function Wd(n,e){let{onStoppedNotification:t}=kn;t&&as.setTimeout(()=>t(n,e))}var qE={closed:!0,next:Hd,error:$E,complete:Hd};var us=typeof Symbol=="function"&&Symbol.observable||"@@observable";function gy(n){return n}function yy(n){return n.length===0?gy:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var kt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=YE(t)?t:new cs(t,i,r);return ls(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=vy(i),new i((r,s)=>{let o=new cs({next:a=>{try{t(a)}catch(l){s(l),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[us](){return this}pipe(...t){return yy(t)(this)}toPromise(t){return t=vy(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function vy(n){var e;return(e=n??kn.Promise)!==null&&e!==void 0?e:Promise}function XE(n){return n&&st(n.next)&&st(n.error)&&st(n.complete)}function YE(n){return n&&n instanceof hr||XE(n)&&fl(n)}function ZE(n){return st(n?.lift)}function ds(n){return e=>{if(ZE(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function fs(n,e,t,i,r){return new $d(n,e,t,i,r)}var $d=class extends hr{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(l){e.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){e.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var _y=ul(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Kn=(()=>{class n extends kt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new ml(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new _y}next(t){ls(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){ls(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){ls(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?Bd:(this.currentObservers=null,s.push(t),new Jt(()=>{this.currentObservers=null,Fo(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new kt;return t.source=this,t}}return n.create=(e,t)=>new ml(e,t),n})(),ml=class extends Kn{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Bd}};var ko=class extends Kn{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function KE(n){return n[n.length-1]}function xy(n){return st(KE(n))?n.pop():void 0}function Ey(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{c(i.next(u))}catch(d){o(d)}}function l(u){try{c(i.throw(u))}catch(d){o(d)}}function c(u){u.done?s(u.value):r(u.value).then(a,l)}c((i=i.apply(n,e||[])).next())})}function My(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function pr(n){return this instanceof pr?(this.v=n,this):new pr(n)}function by(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(f){return function(m){return Promise.resolve(m).then(f,d)}}function a(f,m){i[f]&&(r[f]=function(v){return new Promise(function(g,p){s.push([f,v,g,p])>1||l(f,v)})},m&&(r[f]=m(r[f])))}function l(f,m){try{c(i[f](m))}catch(v){h(s[0][3],v)}}function c(f){f.value instanceof pr?Promise.resolve(f.value.v).then(u,d):h(s[0][2],f)}function u(f){l("next",f)}function d(f){l("throw",f)}function h(f,m){f(m),s.shift(),s.length&&l(s[0][0],s[0][1])}}function wy(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof My=="function"?My(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,l){o=n[s](o),r(a,l,o.done,o.value)})}}function r(s,o,a,l){Promise.resolve(l).then(function(c){s({value:c,done:a})},o)}}var gl=n=>n&&typeof n.length=="number"&&typeof n!="function";function yl(n){return st(n?.then)}function vl(n){return st(n[us])}function _l(n){return Symbol.asyncIterator&&st(n?.[Symbol.asyncIterator])}function xl(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function JE(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Ml=JE();function El(n){return st(n?.[Ml])}function bl(n){return by(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield pr(t.read());if(r)return yield pr(void 0);yield yield pr(i)}}finally{t.releaseLock()}})}function wl(n){return st(n?.getReader)}function ji(n){if(n instanceof kt)return n;if(n!=null){if(vl(n))return QE(n);if(gl(n))return eb(n);if(yl(n))return tb(n);if(_l(n))return Sy(n);if(El(n))return nb(n);if(wl(n))return ib(n)}throw xl(n)}function QE(n){return new kt(e=>{let t=n[us]();if(st(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function eb(n){return new kt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function tb(n){return new kt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,hl)})}function nb(n){return new kt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function Sy(n){return new kt(e=>{rb(n,e).catch(t=>e.error(t))})}function ib(n){return Sy(bl(n))}function rb(n,e){var t,i,r,s;return Ey(this,void 0,void 0,function*(){try{for(t=wy(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function Jn(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function Sl(n,e=0){return ds((t,i)=>{t.subscribe(fs(i,r=>Jn(i,n,()=>i.next(r),e),()=>Jn(i,n,()=>i.complete(),e),r=>Jn(i,n,()=>i.error(r),e)))})}function Cl(n,e=0){return ds((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function Cy(n,e){return ji(n).pipe(Cl(e),Sl(e))}function Ty(n,e){return ji(n).pipe(Cl(e),Sl(e))}function Dy(n,e){return new kt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function Ay(n,e){return new kt(t=>{let i;return Jn(t,e,()=>{i=n[Ml](),Jn(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>st(i?.return)&&i.return()})}function Tl(n,e){if(!n)throw new Error("Iterable cannot be null");return new kt(t=>{Jn(t,e,()=>{let i=n[Symbol.asyncIterator]();Jn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function Iy(n,e){return Tl(bl(n),e)}function Ry(n,e){if(n!=null){if(vl(n))return Cy(n,e);if(gl(n))return Dy(n,e);if(yl(n))return Ty(n,e);if(_l(n))return Tl(n,e);if(El(n))return Ay(n,e);if(wl(n))return Iy(n,e)}throw xl(n)}function qd(n,e){return e?Ry(n,e):ji(n)}function mr(n,e){return ds((t,i)=>{let r=0;t.subscribe(fs(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:sb}=Array;function ob(n,e){return sb(e)?n(...e):n(e)}function Ny(n){return mr(e=>ob(n,e))}var{isArray:ab}=Array,{getPrototypeOf:lb,prototype:cb,keys:ub}=Object;function Py(n){if(n.length===1){let e=n[0];if(ab(e))return{args:e,keys:null};if(db(e)){let t=ub(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function db(n){return n&&typeof n=="object"&&lb(n)===cb}function Oy(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Xd(...n){let e=xy(n),{args:t,keys:i}=Py(n),r=new kt(s=>{let{length:o}=t;if(!o){s.complete();return}let a=new Array(o),l=o,c=o;for(let u=0;u<o;u++){let d=!1;ji(t[u]).subscribe(fs(s,h=>{d||(d=!0,c--),a[u]=h},()=>l--,void 0,()=>{(!l||!d)&&(c||s.next(i?Oy(i,a):a),s.complete())}))}});return e?r.pipe(Ny(e)):r}var fb="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",Pe=class extends Error{code;constructor(e,t){super(pb(e,t)),this.code=e}};function hb(n){return`NG0${Math.abs(n)}`}function pb(n,e){return`${hb(n)}${e?": "+e:""}`}function Jo(n){return{toString:n}.toString()}var Dl="__parameters__";function mb(n){return function(...t){if(n){let i=n(...t);for(let r in i)this[r]=i[r]}}}function Ev(n,e,t){return Jo(()=>{let i=mb(e);function r(...s){if(this instanceof r)return i.apply(this,s),this;let o=new r(...s);return a.annotation=o,a;function a(l,c,u){let d=l.hasOwnProperty(Dl)?l[Dl]:Object.defineProperty(l,Dl,{value:[]})[Dl];for(;d.length<=u;)d.push(null);return(d[u]=d[u]||[]).push(o),l}}return r.prototype.ngMetadataName=n,r.annotationCls=r,r})}var Vo=globalThis;function bt(n){for(let e in n)if(n[e]===bt)return e;throw Error("Could not find renamed property on target object.")}function gb(n,e){for(let t in e)e.hasOwnProperty(t)&&!n.hasOwnProperty(t)&&(n[t]=e[t])}function Dn(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Dn).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function Ly(n,e){return n?e?`${n} ${e}`:n:e||""}var yb=bt({__forward_ref__:bt});function Zi(n){return n.__forward_ref__=Zi,n.toString=function(){return Dn(this())},n}function Qt(n){return bv(n)?n():n}function bv(n){return typeof n=="function"&&n.hasOwnProperty(yb)&&n.__forward_ref__===Zi}function mt(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function bs(n){return{providers:n.providers||[],imports:n.imports||[]}}function ih(n){return Fy(n,wv)||Fy(n,Sv)}function Fy(n,e){return n.hasOwnProperty(e)?n[e]:null}function vb(n){let e=n&&(n[wv]||n[Sv]);return e||null}function ky(n){return n&&(n.hasOwnProperty(Uy)||n.hasOwnProperty(_b))?n[Uy]:null}var wv=bt({\u0275prov:bt}),Uy=bt({\u0275inj:bt}),Sv=bt({ngInjectableDef:bt}),_b=bt({ngInjectorDef:bt}),We=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=mt({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Cv(n){return n&&!!n.\u0275providers}var xb=bt({\u0275cmp:bt}),Mb=bt({\u0275dir:bt}),Eb=bt({\u0275pipe:bt});var Fl=bt({\u0275fac:bt}),zo=bt({__NG_ELEMENT_ID__:bt}),Vy=bt({__NG_ENV_ID__:bt});function kl(n){return typeof n=="string"?n:n==null?"":String(n)}function bb(n){return typeof n=="function"?n.name||n.toString():typeof n=="object"&&n!=null&&typeof n.type=="function"?n.type.name||n.type.toString():kl(n)}function Tv(n,e){throw new Pe(-200,n)}function rh(n,e){throw new Pe(-201,!1)}var Ge=(function(n){return n[n.Default=0]="Default",n[n.Host=1]="Host",n[n.Self=2]="Self",n[n.SkipSelf=4]="SkipSelf",n[n.Optional=8]="Optional",n})(Ge||{}),df;function Dv(){return df}function an(n){let e=df;return df=n,e}function Av(n,e,t){let i=ih(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&Ge.Optional)return null;if(e!==void 0)return e;rh(n,"Injector")}var wb={},yr=wb,ff="__NG_DI_FLAG__",Ul=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=t;return this.injector.get(e,i.optional?cl:yr,i)}},Vl="ngTempTokenPath",Sb="ngTokenPath",Cb=/\n/gm,Tb="\u0275",By="__source";function Db(n,e=Ge.Default){if(Lo()===void 0)throw new Pe(-203,!1);if(Lo()===null)return Av(n,void 0,e);{let t=Lo(),i;return t instanceof Ul?i=t.injector:i=t,i.get(n,e&Ge.Optional?null:void 0,e)}}function ft(n,e=Ge.Default){return(Dv()||Db)(Qt(n),e)}function lt(n,e=Ge.Default){return ft(n,cc(e))}function cc(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function hf(n){let e=[];for(let t=0;t<n.length;t++){let i=Qt(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Pe(900,!1);let r,s=Ge.Default;for(let o=0;o<i.length;o++){let a=i[o],l=Ab(a);typeof l=="number"?l===-1?r=a.token:s|=l:r=a}e.push(ft(r,s))}else e.push(ft(i))}return e}function Iv(n,e){return n[ff]=e,n.prototype[ff]=e,n}function Ab(n){return n[ff]}function Ib(n,e,t,i){let r=n[Vl];throw e[By]&&r.unshift(e[By]),n.message=Rb(`
`+n.message,r,t,i),n[Sb]=r,n[Vl]=null,n}function Rb(n,e,t,i=null){n=n&&n.charAt(0)===`
`&&n.charAt(1)==Tb?n.slice(2):n;let r=Dn(e);if(Array.isArray(e))r=e.map(Dn).join(" -> ");else if(typeof e=="object"){let s=[];for(let o in e)if(e.hasOwnProperty(o)){let a=e[o];s.push(o+":"+(typeof a=="string"?JSON.stringify(a):Dn(a)))}r=`{${s.join(", ")}}`}return`${t}${i?"("+i+")":""}[${r}]: ${n.replace(Cb,`
  `)}`}var Nb=Iv(Ev("Optional"),8);var Pb=Iv(Ev("SkipSelf"),4);function _r(n,e){let t=n.hasOwnProperty(Fl);return t?n[Fl]:null}function Ob(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function Lb(n){return n.flat(Number.POSITIVE_INFINITY)}function sh(n,e){n.forEach(t=>Array.isArray(t)?sh(t,e):e(t))}function Rv(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Bl(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function Fb(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function kb(n,e,t){let i=Qo(n,e);return i>=0?n[i|1]=t:(i=~i,Fb(n,i,e,t)),i}function Yd(n,e){let t=Qo(n,e);if(t>=0)return n[t|1]}function Qo(n,e){return Ub(n,e,1)}function Ub(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var xr={},Cn=[],Hl=new We(""),Nv=new We("",-1),Pv=new We(""),zl=class{get(e,t=yr){if(t===yr){let i=new Error(`NullInjectorError: No provider for ${Dn(e)}!`);throw i.name="NullInjectorError",i}return t}};function Go(n){return n[xb]||null}function Vb(n){return n[Mb]||null}function Bb(n){return n[Eb]||null}function Hb(...n){return{\u0275providers:Ov(!0,n),\u0275fromNgModule:!0}}function Ov(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return sh(e,o=>{let a=o;pf(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&Lv(r,s),t}function Lv(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];oh(r,s=>{e(s,i)})}}function pf(n,e,t,i){if(n=Qt(n),!n)return!1;let r=null,s=ky(n),o=!s&&Go(n);if(!s&&!o){let l=n.ngModule;if(s=ky(l),s)r=l;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let l=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let c of l)pf(c,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let c;sh(s.imports,u=>{pf(u,e,t,i)&&(c||=[],c.push(u))}),c!==void 0&&Lv(c,e)}if(!a){let c=_r(r)||(()=>new r);e({provide:r,useFactory:c,deps:Cn},r),e({provide:Pv,useValue:r,multi:!0},r),e({provide:Hl,useValue:()=>ft(r),multi:!0},r)}let l=s.providers;if(l!=null&&!a){let c=n;oh(l,u=>{e(u,c)})}}else return!1;return r!==n&&n.providers!==void 0}function oh(n,e){for(let t of n)Cv(t)&&(t=t.\u0275providers),Array.isArray(t)?oh(t,e):e(t)}var zb=bt({provide:String,useValue:bt});function Fv(n){return n!==null&&typeof n=="object"&&zb in n}function Gb(n){return!!(n&&n.useExisting)}function Wb(n){return!!(n&&n.useFactory)}function ys(n){return typeof n=="function"}function jb(n){return!!n.useClass}var uc=new We(""),Il={},Hy={},Zd;function ah(){return Zd===void 0&&(Zd=new zl),Zd}var Xi=class{},Wo=class extends Xi{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,gf(e,o=>this.processProvider(o)),this.records.set(Nv,hs(void 0,this)),r.has("environment")&&this.records.set(Xi,hs(void 0,this));let s=this.records.get(uc);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(Pv,Cn,Ge.Self))}retrieve(e,t){let i=t;return this.get(e,i.optional?cl:yr,i)}destroy(){Bo(this),this._destroyed=!0;let e=$e(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),$e(e)}}onDestroy(e){return Bo(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Bo(this);let t=_i(this),i=an(void 0),r;try{return e()}finally{_i(t),an(i)}}get(e,t=yr,i=Ge.Default){if(Bo(this),e.hasOwnProperty(Vy))return e[Vy](this);i=cc(i);let r,s=_i(this),o=an(void 0);try{if(!(i&Ge.SkipSelf)){let l=this.records.get(e);if(l===void 0){let c=Zb(e)&&ih(e);c&&this.injectableDefInScope(c)?l=hs(mf(e),Il):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,i)}let a=i&Ge.Self?ah():this.parent;return t=i&Ge.Optional&&t===yr?null:t,a.get(e,t)}catch(a){if(a.name==="NullInjectorError"){if((a[Vl]=a[Vl]||[]).unshift(Dn(e)),s)throw a;return Ib(a,e,"R3InjectorError",this.source)}else throw a}finally{an(o),_i(s)}}resolveInjectorInitializers(){let e=$e(null),t=_i(this),i=an(void 0),r;try{let s=this.get(Hl,Cn,Ge.Self);for(let o of s)o()}finally{_i(t),an(i),$e(e)}}toString(){let e=[],t=this.records;for(let i of t.keys())e.push(Dn(i));return`R3Injector[${e.join(", ")}]`}processProvider(e){e=Qt(e);let t=ys(e)?e:Qt(e&&e.provide),i=qb(e);if(!ys(e)&&e.multi===!0){let r=this.records.get(t);r||(r=hs(void 0,Il,!0),r.factory=()=>hf(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=$e(null);try{return t.value===Hy?Tv(Dn(e)):t.value===Il&&(t.value=Hy,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&Yb(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{$e(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=Qt(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function mf(n){let e=ih(n),t=e!==null?e.factory:_r(n);if(t!==null)return t;if(n instanceof We)throw new Pe(204,!1);if(n instanceof Function)return $b(n);throw new Pe(204,!1)}function $b(n){if(n.length>0)throw new Pe(204,!1);let t=vb(n);return t!==null?()=>t.factory(n):()=>new n}function qb(n){if(Fv(n))return hs(void 0,n.useValue);{let e=kv(n);return hs(e,Il)}}function kv(n,e,t){let i;if(ys(n)){let r=Qt(n);return _r(r)||mf(r)}else if(Fv(n))i=()=>Qt(n.useValue);else if(Wb(n))i=()=>n.useFactory(...hf(n.deps||[]));else if(Gb(n))i=(r,s)=>ft(Qt(n.useExisting),s!==void 0&&s&Ge.Optional?Ge.Optional:void 0);else{let r=Qt(n&&(n.useClass||n.provide));if(Xb(n))i=()=>new r(...hf(n.deps));else return _r(r)||mf(r)}return i}function Bo(n){if(n.destroyed)throw new Pe(205,!1)}function hs(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function Xb(n){return!!n.deps}function Yb(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function Zb(n){return typeof n=="function"||typeof n=="object"&&n instanceof We}function gf(n,e){for(let t of n)Array.isArray(t)?gf(t,e):t&&Cv(t)?gf(t.\u0275providers,e):e(t)}function Uv(n,e){let t;n instanceof Wo?(Bo(n),t=n):t=new Ul(n);let i,r=_i(t),s=an(void 0);try{return e()}finally{_i(r),an(s)}}function Kb(){return Dv()!==void 0||Lo()!=null}function Jb(n){return typeof n=="function"}var bi=0,Ve=1,Re=2,$t=3,Vn=4,Bn=5,Gl=6,Wl=7,ln=8,vs=9,Yi=10,Ut=11,jo=12,zy=13,ws=14,ei=15,Mr=16,ps=17,xi=18,dc=19,Vv=20,$i=21,Kd=22,jl=23,An=24,Jd=25,In=26,Bv=1;var Er=7,$l=8,_s=9,pn=10;function qi(n){return Array.isArray(n)&&typeof n[Bv]=="object"}function wi(n){return Array.isArray(n)&&n[Bv]===!0}function Hv(n){return(n.flags&4)!==0}function Ss(n){return n.componentOffset>-1}function lh(n){return(n.flags&1)===1}function ti(n){return!!n.template}function ql(n){return(n[Re]&512)!==0}function Cs(n){return(n[Re]&256)===256}var yf=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function zv(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var ch=(()=>{let n=()=>Gv;return n.ngInherit=!0,n})();function Gv(n){return n.type.prototype.ngOnChanges&&(n.setInput=ew),Qb}function Qb(){let n=jv(this),e=n?.current;if(e){let t=n.previous;if(t===xr)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function ew(n,e,t,i,r){let s=this.declaredInputs[i],o=jv(n)||tw(n,{previous:xr,current:null}),a=o.current||(o.current={}),l=o.previous,c=l[s];a[s]=new yf(c&&c.currentValue,t,l===xr),zv(n,e,r,t)}var Wv="__ngSimpleChanges__";function jv(n){return n[Wv]||null}function tw(n,e){return n[Wv]=e}var Gy=null;var vt=function(n,e=null,t){Gy?.(n,e,t)},nw="svg",iw="math";function ni(n){for(;Array.isArray(n);)n=n[bi];return n}function $v(n,e){return ni(e[n])}function Si(n,e){return ni(e[n.index])}function qv(n,e){return n.data[e]}function Xv(n,e){return n[e]}function rw(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function ii(n,e){let t=e[n];return qi(t)?t:t[bi]}function sw(n){return(n[Re]&4)===4}function uh(n){return(n[Re]&128)===128}function ow(n){return wi(n[$t])}function Xl(n,e){return e==null?null:n[e]}function Yv(n){n[ps]=0}function Zv(n){n[Re]&1024||(n[Re]|=1024,uh(n)&&hc(n))}function aw(n,e){for(;n>0;)e=e[ws],n--;return e}function fc(n){return!!(n[Re]&9216||n[An]?.dirty)}function vf(n){n[Yi].changeDetectionScheduler?.notify(8),n[Re]&64&&(n[Re]|=1024),fc(n)&&hc(n)}function hc(n){n[Yi].changeDetectionScheduler?.notify(0);let e=br(n);for(;e!==null&&!(e[Re]&8192||(e[Re]|=8192,!uh(e)));)e=br(e)}function Kv(n,e){if(Cs(n))throw new Pe(911,!1);n[$i]===null&&(n[$i]=[]),n[$i].push(e)}function lw(n,e){if(n[$i]===null)return;let t=n[$i].indexOf(e);t!==-1&&n[$i].splice(t,1)}function br(n){let e=n[$t];return wi(e)?e[$t]:e}function dh(n){return n[Wl]??=[]}function fh(n){return n.cleanup??=[]}function cw(n,e,t,i){let r=dh(e);r.push(t),n.firstCreatePass&&fh(n).push(i,r.length-1)}var Be={lFrame:a_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var _f=!1;function uw(){return Be.lFrame.elementDepthCount}function dw(){Be.lFrame.elementDepthCount++}function fw(){Be.lFrame.elementDepthCount--}function Jv(){return Be.bindingsEnabled}function hw(){return Be.skipHydrationRootTNode!==null}function pw(n){return Be.skipHydrationRootTNode===n}function mw(){Be.skipHydrationRootTNode=null}function ct(){return Be.lFrame.lView}function en(){return Be.lFrame.tView}function si(n){return Be.lFrame.contextLView=n,n[ln]}function oi(n){return Be.lFrame.contextLView=null,n}function Rn(){let n=Qv();for(;n!==null&&n.type===64;)n=n.parent;return n}function Qv(){return Be.lFrame.currentTNode}function gw(){let n=Be.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function ea(n,e){let t=Be.lFrame;t.currentTNode=n,t.isParent=e}function e_(){return Be.lFrame.isParent}function yw(){Be.lFrame.isParent=!1}function t_(){return _f}function Wy(n){let e=_f;return _f=n,e}function n_(){let n=Be.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function vw(){return Be.lFrame.bindingIndex}function _w(n){return Be.lFrame.bindingIndex=n}function hh(){return Be.lFrame.bindingIndex++}function i_(n){let e=Be.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function xw(){return Be.lFrame.inI18n}function Mw(n,e){let t=Be.lFrame;t.bindingIndex=t.bindingRootIndex=n,xf(e)}function Ew(){return Be.lFrame.currentDirectiveIndex}function xf(n){Be.lFrame.currentDirectiveIndex=n}function bw(n){let e=Be.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function r_(){return Be.lFrame.currentQueryIndex}function ph(n){Be.lFrame.currentQueryIndex=n}function ww(n){let e=n[Ve];return e.type===2?e.declTNode:e.type===1?n[Bn]:null}function s_(n,e,t){if(t&Ge.SkipSelf){let r=e,s=n;for(;r=r.parent,r===null&&!(t&Ge.Host);)if(r=ww(s),r===null||(s=s[ws],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=Be.lFrame=o_();return i.currentTNode=e,i.lView=n,!0}function mh(n){let e=o_(),t=n[Ve];Be.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function o_(){let n=Be.lFrame,e=n===null?null:n.child;return e===null?a_(n):e}function a_(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function l_(){let n=Be.lFrame;return Be.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var c_=l_;function gh(){let n=l_();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function Sw(n){return(Be.lFrame.contextLView=aw(n,Be.lFrame.contextLView))[ln]}function Ar(){return Be.lFrame.selectedIndex}function wr(n){Be.lFrame.selectedIndex=n}function u_(){let n=Be.lFrame;return qv(n.tView,n.selectedIndex)}function d_(){return Be.lFrame.currentNamespace}var f_=!0;function yh(){return f_}function vh(n){f_=n}function Cw(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=Gv(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function h_(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),l&&(n.viewHooks??=[]).push(-t,l),c&&((n.viewHooks??=[]).push(t,c),(n.viewCheckHooks??=[]).push(t,c)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function Rl(n,e,t){p_(n,e,3,t)}function Nl(n,e,t,i){(n[Re]&3)===t&&p_(n,e,t,i)}function Qd(n,e){let t=n[Re];(t&3)===e&&(t&=16383,t+=1,n[Re]=t)}function p_(n,e,t,i){let r=i!==void 0?n[ps]&65535:0,s=i??-1,o=e.length-1,a=0;for(let l=r;l<o;l++)if(typeof e[l+1]=="number"){if(a=e[l],i!=null&&a>=i)break}else e[l]<0&&(n[ps]+=65536),(a<s||s==-1)&&(Tw(n,t,e,l),n[ps]=(n[ps]&4294901760)+l+2),l++}function jy(n,e){vt(4,n,e);let t=$e(null);try{e.call(n)}finally{$e(t),vt(5,n,e)}}function Tw(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Re]>>14<n[ps]>>16&&(n[Re]&3)===e&&(n[Re]+=16384,jy(a,s)):jy(a,s)}var gs=-1,Sr=class{factory;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i){this.factory=e,this.canSeeViewProviders=t,this.injectImpl=i}};function Dw(n){return(n.flags&8)!==0}function Aw(n){return(n.flags&16)!==0}function Iw(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];Nw(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function Rw(n){return n===3||n===4||n===6}function Nw(n){return n.charCodeAt(0)===64}function $o(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?$y(n,t,r,null,e[++i]):$y(n,t,r,null,null))}}return n}function $y(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function m_(n){return n!==gs}function Yl(n){return n&32767}function Pw(n){return n>>16}function Zl(n,e){let t=Pw(n),i=e;for(;t>0;)i=i[ws],t--;return i}var Mf=!0;function Kl(n){let e=Mf;return Mf=n,e}var Ow=256,g_=Ow-1,y_=5,Lw=0,Qn={};function Fw(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(zo)&&(i=t[zo]),i==null&&(i=t[zo]=Lw++);let r=i&g_,s=1<<r;e.data[n+(r>>y_)]|=s}function Jl(n,e){let t=v_(n,e);if(t!==-1)return t;let i=e[Ve];i.firstCreatePass&&(n.injectorIndex=e.length,ef(i.data,n),ef(e,null),ef(i.blueprint,null));let r=_h(n,e),s=n.injectorIndex;if(m_(r)){let o=Yl(r),a=Zl(r,e),l=a[Ve].data;for(let c=0;c<8;c++)e[s+c]=a[o+c]|l[o+c]}return e[s+8]=r,s}function ef(n,e){n.push(0,0,0,0,0,0,0,0,e)}function v_(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function _h(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=b_(r),i===null)return gs;if(t++,r=r[ws],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return gs}function Ef(n,e,t){Fw(n,e,t)}function __(n,e,t){if(t&Ge.Optional||n!==void 0)return n;rh(e,"NodeInjector")}function x_(n,e,t,i){if(t&Ge.Optional&&i===void 0&&(i=null),(t&(Ge.Self|Ge.Host))===0){let r=n[vs],s=an(void 0);try{return r?r.get(e,i,t&Ge.Optional):Av(e,i,t&Ge.Optional)}finally{an(s)}}return __(i,e,t)}function M_(n,e,t,i=Ge.Default,r){if(n!==null){if(e[Re]&2048&&!(i&Ge.Self)){let o=Bw(n,e,t,i,Qn);if(o!==Qn)return o}let s=E_(n,e,t,i,Qn);if(s!==Qn)return s}return x_(e,t,i,r)}function E_(n,e,t,i,r){let s=Uw(t);if(typeof s=="function"){if(!s_(e,n,i))return i&Ge.Host?__(r,t,i):x_(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&Ge.Optional))rh(t);else return o}finally{c_()}}else if(typeof s=="number"){let o=null,a=v_(n,e),l=gs,c=i&Ge.Host?e[ei][Bn]:null;for((a===-1||i&Ge.SkipSelf)&&(l=a===-1?_h(n,e):e[a+8],l===gs||!Xy(i,!1)?a=-1:(o=e[Ve],a=Yl(l),e=Zl(l,e)));a!==-1;){let u=e[Ve];if(qy(s,a,u.data)){let d=kw(a,e,t,o,i,c);if(d!==Qn)return d}l=e[a+8],l!==gs&&Xy(i,e[Ve].data[a+8]===c)&&qy(s,a,e)?(o=u,a=Yl(l),e=Zl(l,e)):a=-1}}return r}function kw(n,e,t,i,r,s){let o=e[Ve],a=o.data[n+8],l=i==null?Ss(a)&&Mf:i!=o&&(a.type&3)!==0,c=r&Ge.Host&&s===a,u=Pl(a,o,t,l,c);return u!==null?qo(e,o,u,a,r):Qn}function Pl(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,l=n.directiveStart,c=n.directiveEnd,u=s>>20,d=i?a:a+u,h=r?a+u:c;for(let f=d;f<h;f++){let m=o[f];if(f<l&&t===m||f>=l&&m.type===t)return f}if(r){let f=o[l];if(f&&ti(f)&&f.type===t)return l}return null}function qo(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof Sr){let a=s;a.resolving&&Tv(bb(o[t]));let l=Kl(a.canSeeViewProviders);a.resolving=!0;let c,u=a.injectImpl?an(a.injectImpl):null,d=s_(n,i,Ge.Default);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&Cw(t,o[t],e)}finally{u!==null&&an(u),Kl(l),a.resolving=!1,c_()}}return s}function Uw(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(zo)?n[zo]:void 0;return typeof e=="number"?e>=0?e&g_:Vw:e}function qy(n,e,t){let i=1<<n;return!!(t[e+(n>>y_)]&i)}function Xy(n,e){return!(n&Ge.Self)&&!(n&Ge.Host&&e)}var vr=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return M_(this._tNode,this._lView,e,cc(i),t)}};function Vw(){return new vr(Rn(),ct())}function ta(n){return Jo(()=>{let e=n.prototype.constructor,t=e[Fl]||bf(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[Fl]||bf(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function bf(n){return bv(n)?()=>{let e=bf(Qt(n));return e&&e()}:_r(n)}function Bw(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Re]&2048&&!ql(o);){let a=E_(s,o,t,i|Ge.Self,Qn);if(a!==Qn)return a;let l=s.parent;if(!l){let c=o[Vv];if(c){let u=c.get(t,Qn,i);if(u!==Qn)return u}l=b_(o),o=o[ws]}s=l}return r}function b_(n){let e=n[Ve],t=e.type;return t===2?e.declTNode:t===1?n[Bn]:null}function Yy(n,e=null,t=null,i){let r=Hw(n,e,t,i);return r.resolveInjectorInitializers(),r}function Hw(n,e=null,t=null,i,r=new Set){let s=[t||Cn,Hb(n)];return i=i||(typeof n=="object"?void 0:Dn(n)),new Wo(s,e||ah(),i||null,r)}var xs=class n{static THROW_IF_NOT_FOUND=yr;static NULL=new zl;static create(e,t){if(Array.isArray(e))return Yy({name:""},t,e,"");{let i=e.name??"";return Yy({name:i},e.parent,e.providers,i)}}static \u0275prov=mt({token:n,providedIn:"any",factory:()=>ft(Nv)});static __NG_ELEMENT_ID__=-1};var zw=new We("");zw.__NG_ELEMENT_ID__=n=>{let e=Rn();if(e===null)throw new Pe(204,!1);if(e.type&2)return e.value;if(n&Ge.Optional)return null;throw new Pe(204,!1)};var w_=!1,S_=(()=>{class n{static __NG_ELEMENT_ID__=Gw;static __NG_ENV_ID__=t=>t}return n})(),wf=class extends S_{_lView;constructor(e){super(),this._lView=e}onDestroy(e){let t=this._lView;return Cs(t)?(e(),()=>{}):(Kv(t,e),()=>lw(t,e))}};function Gw(){return new wf(ct())}var Xo=class{},C_=new We("",{providedIn:"root",factory:()=>!1});var T_=new We(""),D_=new We(""),pc=(()=>{class n{taskId=0;pendingTasks=new Set;get _hasPendingTasks(){return this.hasPendingTasks.value}hasPendingTasks=new ko(!1);add(){this._hasPendingTasks||this.hasPendingTasks.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.pendingTasks.size===0&&this._hasPendingTasks&&this.hasPendingTasks.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this._hasPendingTasks&&this.hasPendingTasks.next(!1)}static \u0275prov=mt({token:n,providedIn:"root",factory:()=>new n})}return n})();var Sf=class extends Kn{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,Kb()&&(this.destroyRef=lt(S_,{optional:!0})??void 0,this.pendingTasks=lt(pc,{optional:!0})??void 0)}emit(e){let t=$e(null);try{super.next(e)}finally{$e(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let l=e;r=l.next?.bind(l),s=l.error?.bind(l),o=l.complete?.bind(l)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof Jt&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Tn=Sf;function Ql(...n){}function A_(n){let e,t;function i(){n=Ql;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function Zy(n){return queueMicrotask(()=>n()),()=>{n=Ql}}var xh="isAngularZone",ec=xh+"_ID",Ww=0,Ht=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Tn(!1);onMicrotaskEmpty=new Tn(!1);onStable=new Tn(!1);onError=new Tn(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=w_}=e;if(typeof Zone>"u")throw new Pe(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,qw(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(xh)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Pe(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Pe(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,jw,Ql,Ql);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},jw={};function Mh(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function $w(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){A_(()=>{n.callbackScheduled=!1,Cf(n),n.isCheckStableRunning=!0,Mh(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Cf(n)}function qw(n){let e=()=>{$w(n)},t=Ww++;n._inner=n._inner.fork({name:"angular",properties:{[xh]:!0,[ec]:t,[ec+t]:!0},onInvokeTask:(i,r,s,o,a,l)=>{if(Xw(l))return i.invokeTask(s,o,a,l);try{return Ky(n),i.invokeTask(s,o,a,l)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),Jy(n)}},onInvoke:(i,r,s,o,a,l,c)=>{try{return Ky(n),i.invoke(s,o,a,l,c)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!Yw(l)&&e(),Jy(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Cf(n),Mh(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Cf(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function Ky(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function Jy(n){n._nesting--,Mh(n)}var Tf=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Tn;onMicrotaskEmpty=new Tn;onStable=new Tn;onError=new Tn;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function Xw(n){return I_(n,"__ignore_ng_zone__")}function Yw(n){return I_(n,"__scheduler_tick__")}function I_(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var Mi=class{_console=console;handleError(e){this._console.error("ERROR",e)}},Zw=new We("",{providedIn:"root",factory:()=>{let n=lt(Ht),e=lt(Mi);return t=>n.runOutsideAngular(()=>e.handleError(t))}});function Kw(){return Ts(Rn(),ct())}function Ts(n,e){return new ai(Si(n,e))}var ai=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=Kw}return n})();function Jw(n){return n instanceof ai?n.nativeElement:n}function Qw(n){return typeof n=="function"&&n[Fn]!==void 0}function mc(n,e){let t=Ld(n,e?.equal),i=t[Fn];return t.set=r=>ll(i,r),t.update=r=>Fd(i,r),t.asReadonly=eS.bind(t),t}function eS(){let n=this[Fn];if(n.readonlyFn===void 0){let e=()=>this();e[Fn]=n,n.readonlyFn=e}return n.readonlyFn}function R_(n){return Qw(n)&&typeof n.set=="function"}function tS(){return this._results[Symbol.iterator]()}var Df=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Kn}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=Lb(e);(this._changesDetected=!Ob(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=tS};function N_(n){return(n.flags&128)===128}var P_=(function(n){return n[n.OnPush=0]="OnPush",n[n.Default=1]="Default",n})(P_||{}),O_=new Map,nS=0;function iS(){return nS++}function rS(n){O_.set(n[dc],n)}function Af(n){O_.delete(n[dc])}var Qy="__ngContext__";function na(n,e){qi(e)?(n[Qy]=e[dc],rS(e)):n[Qy]=e}function L_(n){return k_(n[jo])}function F_(n){return k_(n[Vn])}function k_(n){for(;n!==null&&!wi(n);)n=n[Vn];return n}var If;function U_(n){If=n}function sS(){if(If!==void 0)return If;if(typeof document<"u")return document;throw new Pe(210,!1)}var Eh=new We("",{providedIn:"root",factory:()=>oS}),oS="ng",bh=new We(""),ia=new We("",{providedIn:"platform",factory:()=>"unknown"});var wh=new We("",{providedIn:"root",factory:()=>sS().body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var aS="h",lS="b";var V_=!1,cS=new We("",{providedIn:"root",factory:()=>V_});var B_=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(B_||{}),gc=new We(""),ev=new Set;function uS(n){ev.has(n)||(ev.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var dS=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=mt({token:n,providedIn:"root",factory:()=>new n})}return n})();var fS=(n,e,t,i)=>{};function hS(n,e,t,i){fS(n,e,t,i)}var pS=()=>null;function H_(n,e,t=!1){return pS(n,e,t)}function z_(n,e){let t=n.contentQueries;if(t!==null){let i=$e(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];ph(s),a.contentQueries(2,e[o],o)}}}finally{$e(i)}}}function Rf(n,e,t){ph(0);let i=$e(null);try{e(n,t)}finally{$e(i)}}function G_(n,e,t){if(Hv(e)){let i=$e(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let l=t[o];a.contentQueries(1,l,o)}}}finally{$e(i)}}}var ri=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n})(ri||{});var Nf=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${fb})`}};function mS(n){return n instanceof Nf?n.changingThisBreaksApplicationSecurity:n}function gS(n,e){return n.createText(e)}function yS(n,e,t){n.setValue(e,t)}function W_(n,e,t){return n.createElement(e,t)}function tc(n,e,t,i,r){n.insertBefore(e,t,i,r)}function j_(n,e,t){n.appendChild(e,t)}function tv(n,e,t,i,r){i!==null?tc(n,e,t,i,r):j_(n,e,t)}function vS(n,e,t){n.removeChild(null,e,t)}function _S(n,e,t){n.setAttribute(e,"style",t)}function xS(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function $_(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&Iw(n,e,i),r!==null&&xS(n,e,r),s!==null&&_S(n,e,s)}function MS(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var q_="ng-template";function ES(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&MS(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Sh(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function Sh(n){return n.type===4&&n.value!==q_}function bS(n,e,t){let i=n.type===4&&!t?q_:n.value;return e===i}function wS(n,e,t){let i=4,r=n.attrs,s=r!==null?TS(r):0,o=!1;for(let a=0;a<e.length;a++){let l=e[a];if(typeof l=="number"){if(!o&&!Un(i)&&!Un(l))return!1;if(o&&Un(l))continue;o=!1,i=l|i&1;continue}if(!o)if(i&4){if(i=2|i&1,l!==""&&!bS(n,l,t)||l===""&&e.length===1){if(Un(i))return!1;o=!0}}else if(i&8){if(r===null||!ES(n,r,l,t)){if(Un(i))return!1;o=!0}}else{let c=e[++a],u=SS(l,r,Sh(n),t);if(u===-1){if(Un(i))return!1;o=!0;continue}if(c!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&c!==d){if(Un(i))return!1;o=!0}}}}return Un(i)||o}function Un(n){return(n&1)===0}function SS(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return DS(e,n)}function CS(n,e,t=!1){for(let i=0;i<e.length;i++)if(wS(n,e[i],t))return!0;return!1}function TS(n){for(let e=0;e<n.length;e++){let t=n[e];if(Rw(t))return e}return n.length}function DS(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function nv(n,e){return n?":not("+e.trim()+")":e}function AS(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!Un(o)&&(e+=nv(s,r),r=""),i=o,s=s||!Un(i);t++}return r!==""&&(e+=nv(s,r)),e}function IS(n){return n.map(AS).join(",")}function RS(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!Un(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var Ci={};function Ch(n,e,t,i,r,s,o,a,l,c,u){let d=In+i,h=d+r,f=NS(d,h),m=typeof c=="function"?c():c;return f[Ve]={type:n,blueprint:f,template:t,queries:null,viewQuery:a,declTNode:e,data:f.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:l,consts:m,incompleteFirstPass:!1,ssrId:u}}function NS(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:Ci);return t}function PS(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Ch(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Th(n,e,t,i,r,s,o,a,l,c,u){let d=e.blueprint.slice();return d[bi]=r,d[Re]=i|4|128|8|64|1024,(c!==null||n&&n[Re]&2048)&&(d[Re]|=2048),Yv(d),d[$t]=d[ws]=n,d[ln]=t,d[Yi]=o||n&&n[Yi],d[Ut]=a||n&&n[Ut],d[vs]=l||n&&n[vs]||null,d[Bn]=s,d[dc]=iS(),d[Gl]=u,d[Vv]=c,d[ei]=e.type==2?n[ei]:d,d}function OS(n,e,t){let i=Si(e,n),r=PS(t),s=n[Yi].rendererFactory,o=Dh(n,Th(n,r,null,X_(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function X_(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function Y_(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Dh(n,e){return n[jo]?n[zy][Vn]=e:n[jo]=e,n[zy]=e,e}function Dt(n=1){Z_(en(),ct(),Ar()+n,!1)}function Z_(n,e,t,i){if(!i)if((e[Re]&3)===3){let s=n.preOrderCheckHooks;s!==null&&Rl(e,s,t)}else{let s=n.preOrderHooks;s!==null&&Nl(e,s,0,t)}wr(t)}var yc=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(yc||{});function Pf(n,e,t,i){let r=$e(null);try{let[s,o,a]=n.inputs[t],l=null;(o&yc.SignalBased)!==0&&(l=e[s][Fn]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,l,i,t,s):zv(e,l,s,i)}finally{$e(r)}}function K_(n,e,t,i,r){let s=Ar(),o=i&2;try{wr(-1),o&&e.length>In&&Z_(n,e,In,!1),vt(o?2:0,r),t(i,r)}finally{wr(s),vt(o?3:1,r)}}function Ah(n,e,t){BS(n,e,t),(t.flags&64)===64&&HS(n,e,t)}function J_(n,e,t=Si){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function LS(n,e,t,i){let s=i.get(cS,V_)||t===ri.ShadowDom,o=n.selectRootElement(e,s);if(o?.tagName?.toLowerCase()==="script")throw new Pe(905,!1);return FS(o),o}function FS(n){kS(n)}var kS=()=>null;function US(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function Q_(n,e,t,i,r,s,o,a){if(!a&&Ih(e,n,t,i,r)){Ss(e)&&VS(t,e.index);return}if(e.type&3){let l=Si(e,t);i=US(i),r=o!=null?o(r,e.value||"",i):r,s.setProperty(l,i,r)}else e.type&12}function VS(n,e){let t=ii(e,n);t[Re]&16||(t[Re]|=64)}function BS(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Ss(t)&&OS(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Jl(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],l=qo(e,n,o,t);if(na(l,e),s!==null&&GS(e,o-i,l,a,t,s),ti(a)){let c=ii(t.index,e);c[ln]=qo(e,n,o,t)}}}function HS(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=Ew();try{wr(s);for(let a=i;a<r;a++){let l=n.data[a],c=e[a];xf(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&zS(l,c)}}finally{wr(-1),xf(o)}}function zS(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function e0(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];CS(e,s.selectors,!1)&&(i??=[],ti(s)?i.unshift(s):i.push(s))}return i}function GS(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let l=o[a],c=o[a+1];Pf(i,t,l,c)}}function WS(n,e){let t=n[vs],i=t?t.get(Mi,null):null;i&&i.handleError(e)}function Ih(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let l=0;l<o.length;l+=2){let c=o[l],u=o[l+1],d=e.data[c];Pf(d,t[c],u,r),a=!0}if(s)for(let l of s){let c=t[l],u=e.data[l];Pf(u,c,i,r),a=!0}return a}function jS(n,e){let t=ii(e,n),i=t[Ve];$S(i,t);let r=t[bi];r!==null&&t[Gl]===null&&(t[Gl]=H_(r,t[vs])),vt(18),Rh(i,t,t[ln]),vt(19,t[ln])}function $S(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Rh(n,e,t){mh(e);try{let i=n.viewQuery;i!==null&&Rf(1,i,t);let r=n.template;r!==null&&K_(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[xi]?.finishViewCreation(n),n.staticContentQueries&&z_(n,e),n.staticViewQueries&&Rf(2,n.viewQuery,t);let s=n.components;s!==null&&qS(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Re]&=-5,gh()}}function qS(n,e){for(let t=0;t<e.length;t++)jS(n,e[t])}function XS(n,e,t,i){let r=$e(null);try{let s=e.tView,a=n[Re]&4096?4096:16,l=Th(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=n[e.index];l[Mr]=c;let u=n[xi];return u!==null&&(l[xi]=u.createEmbeddedView(s)),Rh(s,l,t),l}finally{$e(r)}}function iv(n,e){return!e||e.firstChild===null||N_(n)}var YS;function Nh(n,e){return YS(n,e)}var Ei=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(Ei||{});function t0(n){return(n.flags&32)===32}function ms(n,e,t,i,r){if(i!=null){let s,o=!1;wi(i)?s=i:qi(i)&&(o=!0,i=i[bi]);let a=ni(i);n===0&&t!==null?r==null?j_(e,t,a):tc(e,t,a,r||null,!0):n===1&&t!==null?tc(e,t,a,r||null,!0):n===2?vS(e,a,o):n===3&&e.destroyNode(a),s!=null&&aC(e,n,s,t,r)}}function ZS(n,e){n0(n,e),e[bi]=null,e[Bn]=null}function KS(n,e,t,i,r,s){i[bi]=r,i[Bn]=e,vc(n,i,t,1,r,s)}function n0(n,e){e[Yi].changeDetectionScheduler?.notify(9),vc(n,e,e[Ut],2,null,null)}function JS(n){let e=n[jo];if(!e)return tf(n[Ve],n);for(;e;){let t=null;if(qi(e))t=e[jo];else{let i=e[pn];i&&(t=i)}if(!t){for(;e&&!e[Vn]&&e!==n;)qi(e)&&tf(e[Ve],e),e=e[$t];e===null&&(e=n),qi(e)&&tf(e[Ve],e),t=e&&e[Vn]}e=t}}function Ph(n,e){let t=n[_s],i=t.indexOf(e);t.splice(i,1)}function i0(n,e){if(Cs(e))return;let t=e[Ut];t.destroyNode&&vc(n,e,t,3,null,null),JS(e)}function tf(n,e){if(Cs(e))return;let t=$e(null);try{e[Re]&=-129,e[Re]|=256,e[An]&&Nd(e[An]),eC(n,e),QS(n,e),e[Ve].type===1&&e[Ut].destroy();let i=e[Mr];if(i!==null&&wi(e[$t])){i!==e[$t]&&Ph(i,e);let r=e[xi];r!==null&&r.detachView(n)}Af(e)}finally{$e(t)}}function QS(n,e){let t=n.cleanup,i=e[Wl];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[Wl]=null);let r=e[$i];if(r!==null){e[$i]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[jl];if(s!==null){e[jl]=null;for(let o of s)o.destroy()}}function eC(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof Sr)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],l=s[o+1];vt(4,a,l);try{l.call(a)}finally{vt(5,a,l)}}else{vt(4,r,s);try{s.call(r)}finally{vt(5,r,s)}}}}}function tC(n,e,t){return nC(n,e.parent,t)}function nC(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[bi];if(Ss(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===ri.None||r===ri.Emulated)return null}return Si(i,t)}function iC(n,e,t){return sC(n,e,t)}function rC(n,e,t){return n.type&40?Si(n,t):null}var sC=rC,rv;function Oh(n,e,t,i){let r=tC(n,i,e),s=e[Ut],o=i.parent||e[Bn],a=iC(o,i,e);if(r!=null)if(Array.isArray(t))for(let l=0;l<t.length;l++)tv(s,r,t[l],a,!1);else tv(s,r,t,a,!1);rv!==void 0&&rv(s,i,e,t,r)}function Ho(n,e){if(e!==null){let t=e.type;if(t&3)return Si(e,n);if(t&4)return Of(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return Ho(n,i);{let r=n[e.index];return wi(r)?Of(-1,r):ni(r)}}else{if(t&128)return Ho(n,e.next);if(t&32)return Nh(e,n)()||ni(n[e.index]);{let i=r0(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=br(n[ei]);return Ho(r,i)}else return Ho(n,e.next)}}}return null}function r0(n,e){if(e!==null){let i=n[ei][Bn],r=e.projection;return i.projection[r]}return null}function Of(n,e){let t=pn+n+1;if(t<e.length){let i=e[t],r=i[Ve].firstChild;if(r!==null)return Ho(i,r)}return e[Er]}function Lh(n,e,t,i,r,s,o){for(;t!=null;){if(t.type===128){t=t.next;continue}let a=i[t.index],l=t.type;if(o&&e===0&&(a&&na(ni(a),i),t.flags|=2),!t0(t))if(l&8)Lh(n,e,t.child,i,r,s,!1),ms(e,n,r,a,s);else if(l&32){let c=Nh(t,i),u;for(;u=c();)ms(e,n,r,u,s);ms(e,n,r,a,s)}else l&16?oC(n,e,i,t,r,s):ms(e,n,r,a,s);t=o?t.projectionNext:t.next}}function vc(n,e,t,i,r,s){Lh(t,i,n.firstChild,e,r,s,!1)}function oC(n,e,t,i,r,s){let o=t[ei],l=o[Bn].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];ms(e,n,r,u,s)}else{let c=l,u=o[$t];N_(i)&&(c.flags|=128),Lh(n,e,c,u,r,s,!0)}}function aC(n,e,t,i,r){let s=t[Er],o=ni(t);s!==o&&ms(e,n,i,s,r);for(let a=pn;a<t.length;a++){let l=t[a];vc(l[Ve],l,n,e,i,s)}}function lC(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:Ei.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=Ei.Important),n.setStyle(t,i,r,s))}}function nc(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(ni(s)),wi(s)&&cC(s,i);let o=t.type;if(o&8)nc(n,e,t.child,i);else if(o&32){let a=Nh(t,e),l;for(;l=a();)i.push(l)}else if(o&16){let a=r0(e,t);if(Array.isArray(a))i.push(...a);else{let l=br(e[ei]);nc(l[Ve],l,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function cC(n,e){for(let t=pn;t<n.length;t++){let i=n[t],r=i[Ve].firstChild;r!==null&&nc(i[Ve],i,r,e)}n[Er]!==n[bi]&&e.push(n[Er])}function s0(n){if(n[Jd]!==null){for(let e of n[Jd])e.impl.addSequence(e);n[Jd].length=0}}var o0=[];function uC(n){return n[An]??dC(n)}function dC(n){let e=o0.pop()??Object.create(hC);return e.lView=n,e}function fC(n){n.lView[An]!==n&&(n.lView=null,o0.push(n))}var hC=St(pt({},Oo),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{hc(n.lView)},consumerOnSignalRead(){this.lView[An]=this}});function pC(n){let e=n[An]??Object.create(mC);return e.lView=n,e}var mC=St(pt({},Oo),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=br(n.lView);for(;e&&!a0(e[Ve]);)e=br(e);e&&Zv(e)},consumerOnSignalRead(){this.lView[An]=this}});function a0(n){return n.type!==2}function l0(n){if(n[jl]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[jl])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Re]&8192)}}var gC=100;function c0(n,e=!0,t=0){let r=n[Yi].rendererFactory,s=!1;s||r.begin?.();try{yC(n,t)}catch(o){throw e&&WS(n,o),o}finally{s||r.end?.()}}function yC(n,e){let t=t_();try{Wy(!0),Lf(n,e);let i=0;for(;fc(n);){if(i===gC)throw new Pe(103,!1);i++,Lf(n,1)}}finally{Wy(t)}}function vC(n,e,t,i){if(Cs(e))return;let r=e[Re],s=!1,o=!1;mh(e);let a=!0,l=null,c=null;s||(a0(n)?(c=uC(e),l=sl(c)):Cd()===null?(a=!1,c=pC(e),l=sl(c)):e[An]&&(Nd(e[An]),e[An]=null));try{Yv(e),_w(n.bindingStartIndex),t!==null&&K_(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let f=n.preOrderCheckHooks;f!==null&&Rl(e,f,null)}else{let f=n.preOrderHooks;f!==null&&Nl(e,f,0,null),Qd(e,0)}if(o||_C(e),l0(e),u0(e,0),n.contentQueries!==null&&z_(n,e),!s)if(u){let f=n.contentCheckHooks;f!==null&&Rl(e,f)}else{let f=n.contentHooks;f!==null&&Nl(e,f,1),Qd(e,1)}MC(n,e);let d=n.components;d!==null&&f0(e,d,0);let h=n.viewQuery;if(h!==null&&Rf(2,h,i),!s)if(u){let f=n.viewCheckHooks;f!==null&&Rl(e,f)}else{let f=n.viewHooks;f!==null&&Nl(e,f,2),Qd(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Kd]){for(let f of e[Kd])f();e[Kd]=null}s||(s0(e),e[Re]&=-73)}catch(u){throw s||hc(e),u}finally{c!==null&&(Id(c,l),a&&fC(c)),gh()}}function u0(n,e){for(let t=L_(n);t!==null;t=F_(t))for(let i=pn;i<t.length;i++){let r=t[i];d0(r,e)}}function _C(n){for(let e=L_(n);e!==null;e=F_(e)){if(!(e[Re]&2))continue;let t=e[_s];for(let i=0;i<t.length;i++){let r=t[i];Zv(r)}}}function xC(n,e,t){vt(18);let i=ii(e,n);d0(i,t),vt(19,i[ln])}function d0(n,e){uh(n)&&Lf(n,e)}function Lf(n,e){let i=n[Ve],r=n[Re],s=n[An],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Rd(s)),o||=!1,s&&(s.dirty=!1),n[Re]&=-9217,o)vC(i,n,i.template,n[ln]);else if(r&8192){l0(n),u0(n,1);let a=i.components;a!==null&&f0(n,a,1),s0(n)}}function f0(n,e,t){for(let i=0;i<e.length;i++)xC(n,e[i],t)}function MC(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)wr(~r);else{let s=r,o=t[++i],a=t[++i];Mw(o,s);let l=e[s];vt(24,l),a(2,l),vt(25,l)}}}finally{wr(-1)}}function Fh(n,e){let t=t_()?64:1088;for(n[Yi].changeDetectionScheduler?.notify(e);n;){n[Re]|=t;let i=br(n);if(ql(n)&&!i)return n;n=i}return null}function h0(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function EC(n,e,t,i=!0){let r=e[Ve];if(bC(r,e,n,t),i){let o=Of(t,n),a=e[Ut],l=a.parentNode(n[Er]);l!==null&&KS(r,n[Bn],a,e,l,o)}let s=e[Gl];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function Ff(n,e){if(n.length<=pn)return;let t=pn+e,i=n[t];if(i){let r=i[Mr];r!==null&&r!==n&&Ph(r,i),e>0&&(n[t-1][Vn]=i[Vn]);let s=Bl(n,pn+e);ZS(i[Ve],i);let o=s[xi];o!==null&&o.detachView(s[Ve]),i[$t]=null,i[Vn]=null,i[Re]&=-129}return i}function bC(n,e,t,i){let r=pn+i,s=t.length;i>0&&(t[r-1][Vn]=e),i<s-pn?(e[Vn]=t[r],Rv(t,pn+i,e)):(t.push(e),e[Vn]=null),e[$t]=t;let o=e[Mr];o!==null&&t!==o&&p0(o,e);let a=e[xi];a!==null&&a.insertView(n),vf(e),e[Re]|=128}function p0(n,e){let t=n[_s],i=e[$t];if(qi(i))n[Re]|=2;else{let r=i[$t][ei];e[ei]!==r&&(n[Re]|=2)}t===null?n[_s]=[e]:t.push(e)}var Yo=class{_lView;_cdRefInjectingView;notifyErrorHandler;_appRef=null;_attachedToViewContainer=!1;get rootNodes(){let e=this._lView,t=e[Ve];return nc(t,e,t.firstChild,[])}constructor(e,t,i=!0){this._lView=e,this._cdRefInjectingView=t,this.notifyErrorHandler=i}get context(){return this._lView[ln]}set context(e){this._lView[ln]=e}get destroyed(){return Cs(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[$t];if(wi(e)){let t=e[$l],i=t?t.indexOf(this):-1;i>-1&&(Ff(e,i),Bl(t,i))}this._attachedToViewContainer=!1}i0(this._lView[Ve],this._lView)}onDestroy(e){Kv(this._lView,e)}markForCheck(){Fh(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Re]&=-129}reattach(){vf(this._lView),this._lView[Re]|=128}detectChanges(){this._lView[Re]|=1024,c0(this._lView,this.notifyErrorHandler)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Pe(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=ql(this._lView),t=this._lView[Mr];t!==null&&!e&&Ph(t,this._lView),n0(this._lView[Ve],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Pe(902,!1);this._appRef=e;let t=ql(this._lView),i=this._lView[Mr];i!==null&&!t&&p0(i,this._lView),vf(this._lView)}};var Cr=(()=>{class n{static __NG_ELEMENT_ID__=CC}return n})(),wC=Cr,SC=class extends wC{_declarationLView;_declarationTContainer;elementRef;constructor(e,t,i){super(),this._declarationLView=e,this._declarationTContainer=t,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,t){return this.createEmbeddedViewImpl(e,t)}createEmbeddedViewImpl(e,t,i){let r=XS(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:t,dehydratedView:i});return new Yo(r)}};function CC(){return kh(Rn(),ct())}function kh(n,e){return n.type&4?new SC(e,n,Ts(n,e)):null}function Uh(n,e,t,i,r){let s=n.data[e];if(s===null)s=TC(n,e,t,i,r),xw()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=gw();s.injectorIndex=o===null?-1:o.injectorIndex}return ea(s,!0),s}function TC(n,e,t,i,r){let s=Qv(),o=e_(),a=o?s:s&&s.parent,l=n.data[e]=AC(n,a,t,e,i,r);return DC(n,l,s,o),l}function DC(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function AC(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return hw()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:d_(),attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var pU=new RegExp(`^(\\d+)*(${lS}|${aS})*(.*)`);var IC=()=>null;function sv(n,e){return IC(n,e)}var RC=class{},m0=class{},kf=class{resolveComponentFactory(e){throw Error(`No component factory found for ${Dn(e)}.`)}},Vh=class{static NULL=new kf},Ms=class{},Ds=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>NC()}return n})();function NC(){let n=ct(),e=Rn(),t=ii(e.index,n);return(qi(t)?t:n)[Ut]}var PC=(()=>{class n{static \u0275prov=mt({token:n,providedIn:"root",factory:()=>null})}return n})();var nf={},Uf=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){i=cc(i);let r=this.injector.get(e,nf,i);return r!==nf||t===nf?r:this.parentInjector.get(e,t,i)}};function ov(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=Ly(r,a);else if(s==2){let l=a,c=e[++o];i=Ly(i,l+": "+c+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Je(n,e=Ge.Default){let t=ct();if(t===null)return ft(n,e);let i=Rn();return M_(i,t,Qt(n),e)}function g0(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a,l=null,c=null,u=LC(o);u===null?a=o:[a,l,c]=u,UC(n,e,t,a,s,l,c)}s!==null&&i!==null&&OC(t,i,s)}function OC(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Pe(-301,!1);i.push(e[r],s)}}function LC(n){let e=null,t=!1;for(let o=0;o<n.length;o++){let a=n[o];if(o===0&&ti(a)&&(e=a),a.findHostDirectiveDefs!==null){t=!0;break}}if(!t)return null;let i=null,r=null,s=null;for(let o of n)o.findHostDirectiveDefs!==null&&(i??=[],r??=new Map,s??=new Map,FC(o,i,s,r)),o===e&&(i??=[],i.push(o));return i!==null?(i.push(...e===null?n:n.slice(1)),[i,r,s]):null}function FC(n,e,t,i){let r=e.length;n.findHostDirectiveDefs(n,e,i),t.set(n,[r,e.length-1])}function kC(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function UC(n,e,t,i,r,s,o){let a=i.length,l=!1;for(let h=0;h<a;h++){let f=i[h];!l&&ti(f)&&(l=!0,kC(n,t,h)),Ef(Jl(t,e),n,f.type)}WC(t,n.data.length,a);for(let h=0;h<a;h++){let f=i[h];f.providersResolver&&f.providersResolver(f)}let c=!1,u=!1,d=Y_(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let h=0;h<a;h++){let f=i[h];if(t.mergedAttrs=$o(t.mergedAttrs,f.hostAttrs),BC(n,t,e,d,f),GC(d,f,r),o!==null&&o.has(f)){let[v,g]=o.get(f);t.directiveToIndex.set(f.type,[d,v+t.directiveStart,g+t.directiveStart])}else(s===null||!s.has(f))&&t.directiveToIndex.set(f.type,d);f.contentQueries!==null&&(t.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(t.flags|=64);let m=f.type.prototype;!c&&(m.ngOnChanges||m.ngOnInit||m.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),c=!0),!u&&(m.ngOnChanges||m.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}VC(n,t,s)}function VC(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))av(0,e,r,i),av(1,e,r,i),cv(e,i,!1);else{let s=t.get(r);lv(0,e,s,i),lv(1,e,s,i),cv(e,i,!0)}}}function av(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),y0(e,s)}}function lv(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),y0(e,o)}}function y0(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function cv(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||Sh(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let l=i[a];if(l===0){a+=4;continue}else if(l===5){a+=2;continue}else if(typeof l=="number")break;if(!t&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===e){o??=[],o.push(l,i[a+1]);break}}else if(t&&s.hasOwnProperty(l)){let c=s[l];for(let u=0;u<c.length;u+=2)if(c[u]===e){o??=[],o.push(c[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function BC(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=_r(r.type,!0)),o=new Sr(s,ti(r),Je);n.blueprint[i]=o,t[i]=o,HC(n,e,i,Y_(n,t,r.hostVars,Ci),r)}function HC(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;zC(o)!=a&&o.push(a),o.push(t,i,s)}}function zC(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function GC(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;ti(e)&&(t[""]=n)}}function WC(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function v0(n,e,t,i,r,s,o,a){let l=e.consts,c=Xl(l,o),u=Uh(e,n,2,i,c);return s&&g0(e,t,u,Xl(l,a),r),u.mergedAttrs=$o(u.mergedAttrs,u.attrs),u.attrs!==null&&ov(u,u.attrs,!1),u.mergedAttrs!==null&&ov(u,u.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,u),u}function _0(n,e){h_(n,e),Hv(e)&&n.queries.elementEnd(e)}var Vf=class extends Vh{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=Go(e);return new ic(t,this.ngModule)}};function jC(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&yc.SignalBased)!==0};return r&&(s.transform=r),s})}function $C(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function qC(n,e,t){let i=e instanceof Xi?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Uf(t,i):t}function XC(n){let e=n.get(Ms,null);if(e===null)throw new Pe(407,!1);let t=n.get(PC,null),i=n.get(Xo,null);return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i}}function YC(n,e){let t=(n.selectors[0][0]||"div").toLowerCase();return W_(e,t,t==="svg"?nw:t==="math"?iw:null)}var ic=class extends m0{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=jC(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=$C(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=IS(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r){vt(22);let s=$e(null);try{let o=this.componentDef,a=i?["ng-version","19.2.25"]:RS(this.componentDef.selectors[0]),l=Ch(0,null,null,1,0,null,null,null,null,[a],null),c=qC(o,r||this.ngModule,e),u=XC(c),d=u.rendererFactory.createRenderer(null,o),h=i?LS(d,i,o.encapsulation,c):YC(o,d),f=Th(null,l,null,512|X_(o),null,null,u,d,c,null,H_(h,c,!0));f[In]=h,mh(f);let m=null;try{let v=v0(In,l,f,"#host",()=>[this.componentDef],!0,0);h&&($_(d,h,v),na(h,f)),Ah(l,f,v),G_(l,v,f),_0(l,v),t!==void 0&&ZC(v,this.ngContentSelectors,t),m=ii(v.index,f),f[ln]=m[ln],Rh(l,f,null)}catch(v){throw m!==null&&Af(m),Af(f),v}finally{vt(23),gh()}return new Bf(this.componentType,f)}finally{$e(s)}}},Bf=class extends RC{_rootLView;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t){super(),this._rootLView=t,this._tNode=qv(t[Ve],In),this.location=Ts(this._tNode,t),this.instance=ii(this._tNode.index,t)[ln],this.hostView=this.changeDetectorRef=new Yo(t,void 0,!1),this.componentType=e}setInput(e,t){let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=Ih(i,r[Ve],r,e,t);this.previousInputValues.set(e,t);let o=ii(i.index,r);Fh(o,1)}get injector(){return new vr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function ZC(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var As=(()=>{class n{static __NG_ELEMENT_ID__=KC}return n})();function KC(){let n=Rn();return M0(n,ct())}var JC=As,x0=class extends JC{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return Ts(this._hostTNode,this._hostLView)}get injector(){return new vr(this._hostTNode,this._hostLView)}get parentInjector(){let e=_h(this._hostTNode,this._hostLView);if(m_(e)){let t=Zl(e,this._hostLView),i=Yl(e),r=t[Ve].data[i+8];return new vr(r,t)}else return new vr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=uv(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-pn}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=sv(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,iv(this._hostTNode,o)),a}createComponent(e,t,i,r,s){let o=e&&!Jb(e),a;if(o)a=t;else{let m=t||{};a=m.index,i=m.injector,r=m.projectableNodes,s=m.environmentInjector||m.ngModuleRef}let l=o?e:new ic(Go(e)),c=i||this.parentInjector;if(!s&&l.ngModule==null){let v=(o?c:this.parentInjector).get(Xi,null);v&&(s=v)}let u=Go(l.componentType??{}),d=sv(this._lContainer,u?.id??null),h=d?.firstChild??null,f=l.create(c,r,h,s);return this.insertImpl(f.hostView,a,iv(this._hostTNode,d)),f}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(ow(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let l=r[$t],c=new x0(l,l[Bn],l[$t]);c.detach(c.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return EC(o,r,s,i),e.attachToViewContainerRef(),Rv(rf(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=uv(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=Ff(this._lContainer,t);i&&(Bl(rf(this._lContainer),t),i0(i[Ve],i))}detach(e){let t=this._adjustIndex(e,-1),i=Ff(this._lContainer,t);return i&&Bl(rf(this._lContainer),t)!=null?new Yo(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function uv(n){return n[$l]}function rf(n){return n[$l]||(n[$l]=[])}function M0(n,e){let t,i=e[n.index];return wi(i)?t=i:(t=h0(i,e,null,n),e[n.index]=t,Dh(e,t)),eT(t,e,n,i),new x0(t,n,e)}function QC(n,e){let t=n[Ut],i=t.createComment(""),r=Si(e,n),s=t.parentNode(r);return tc(t,s,i,t.nextSibling(r),!1),i}var eT=iT,tT=()=>!1;function nT(n,e,t){return tT(n,e,t)}function iT(n,e,t,i){if(n[Er])return;let r;t.type&8?r=ni(i):r=QC(e,t),n[Er]=r}var Hf=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},zf=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Bh(e,t).matches!==null&&this.queries[t].setDirty()}},Gf=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=dT(e):this.predicate=e}},Wf=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},jf=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,rT(t,s)),this.matchTNodeWithReadOption(e,t,Pl(t,e,s,!1,!1))}else i===Cr?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,Pl(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===ai||r===As||r===Cr&&t.type&4)this.addMatch(t.index,-2);else{let s=Pl(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function rT(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function sT(n,e){return n.type&11?Ts(n,e):n.type&4?kh(n,e):null}function oT(n,e,t,i){return t===-1?sT(e,n):t===-2?aT(n,e,i):qo(n,n[Ve],t,e)}function aT(n,e,t){if(t===ai)return Ts(e,n);if(t===Cr)return kh(e,n);if(t===As)return M0(e,n)}function E0(n,e,t,i){let r=e[xi].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let l=0;o!==null&&l<o.length;l+=2){let c=o[l];if(c<0)a.push(null);else{let u=s[c];a.push(oT(e,u,o[l+1],t.metadata.read))}}r.matches=a}return r.matches}function $f(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=E0(n,e,r,t);for(let a=0;a<s.length;a+=2){let l=s[a];if(l>0)i.push(o[a/2]);else{let c=s[a+1],u=e[-l];for(let d=pn;d<u.length;d++){let h=u[d];h[Mr]===h[$t]&&$f(h[Ve],h,c,i)}if(u[_s]!==null){let d=u[_s];for(let h=0;h<d.length;h++){let f=d[h];$f(f[Ve],f,c,i)}}}}}return i}function lT(n,e){return n[xi].queries[e].queryList}function cT(n,e,t){let i=new Df((t&4)===4);return cw(n,e,i,i.destroy),(e[xi]??=new zf).queries.push(new Hf(i))-1}function uT(n,e,t){let i=en();return i.firstCreatePass&&(fT(i,new Gf(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),cT(i,ct(),e)}function dT(n){return n.split(",").map(e=>e.trim())}function fT(n,e,t){n.queries===null&&(n.queries=new Wf),n.queries.track(new jf(e,t))}function Bh(n,e){return n.queries.getByIndex(e)}function hT(n,e){let t=n[Ve],i=Bh(t,e);return i.crossesNgTemplate?$f(t,n,e,[]):E0(t,n,i,e)}var rc=class{};var sc=class extends rc{injector;componentFactoryResolver=new Vf(this);instance=null;constructor(e){super();let t=new Wo([...e.providers,{provide:rc,useValue:this},{provide:Vh,useValue:this.componentFactoryResolver}],e.parent||ah(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function pT(n,e,t=null){return new sc({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var mT=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Ov(!1,t.type),r=i.length>0?pT([i],this._injector,`Standalone[${t.type.name}]`):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=mt({token:n,providedIn:"environment",factory:()=>new n(ft(Xi))})}return n})();function _c(n){return Jo(()=>{let e=w0(n),t=St(pt({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===P_.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(mT).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||ri.Emulated,styles:n.styles||Cn,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&uS("NgStandalone"),S0(t);let i=n.dependencies;return t.directiveDefs=dv(i,!1),t.pipeDefs=dv(i,!0),t.id=xT(t),t})}function gT(n){return Go(n)||Vb(n)}function yT(n){return n!==null}function Is(n){return Jo(()=>({type:n.type,bootstrap:n.bootstrap||Cn,declarations:n.declarations||Cn,imports:n.imports||Cn,exports:n.exports||Cn,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function vT(n,e){if(n==null)return xr;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,l;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,l=r[3]||null):(s=r,o=r,a=yc.None,l=null),t[s]=[i,a,l],e[s]=o}return t}function _T(n){if(n==null)return xr;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function cn(n){return Jo(()=>{let e=w0(n);return S0(e),e})}function b0(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function w0(n){let e={};return{type:n.type,providersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||xr,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Cn,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,findHostDirectiveDefs:null,hostDirectives:null,inputs:vT(n.inputs,e),outputs:_T(n.outputs),debugInfo:null}}function S0(n){n.features?.forEach(e=>e(n))}function dv(n,e){if(!n)return null;let t=e?Bb:gT;return()=>(typeof n=="function"?n():n).map(i=>t(i)).filter(yT)}function xT(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function MT(n){return Object.getPrototypeOf(n.prototype).constructor}function Ti(n){let e=MT(n.type),t=!0,i=[n];for(;e;){let r;if(ti(n))r=e.\u0275cmp||e.\u0275dir;else{if(e.\u0275cmp)throw new Pe(903,!1);r=e.\u0275dir}if(r){if(t){i.push(r);let o=n;o.inputs=sf(n.inputs),o.declaredInputs=sf(n.declaredInputs),o.outputs=sf(n.outputs);let a=r.hostBindings;a&&CT(n,a);let l=r.viewQuery,c=r.contentQueries;if(l&&wT(n,l),c&&ST(n,c),ET(n,r),gb(n.outputs,r.outputs),ti(r)&&r.data.animation){let u=n.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let s=r.features;if(s)for(let o=0;o<s.length;o++){let a=s[o];a&&a.ngInherit&&a(n),a===Ti&&(t=!1)}}e=Object.getPrototypeOf(e)}bT(i)}function ET(n,e){for(let t in e.inputs){if(!e.inputs.hasOwnProperty(t)||n.inputs.hasOwnProperty(t))continue;let i=e.inputs[t];i!==void 0&&(n.inputs[t]=i,n.declaredInputs[t]=e.declaredInputs[t])}}function bT(n){let e=0,t=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=e+=r.hostVars,r.hostAttrs=$o(r.hostAttrs,t=$o(t,r.hostAttrs))}}function sf(n){return n===xr?{}:n===Cn?[]:n}function wT(n,e){let t=n.viewQuery;t?n.viewQuery=(i,r)=>{e(i,r),t(i,r)}:n.viewQuery=e}function ST(n,e){let t=n.contentQueries;t?n.contentQueries=(i,r,s)=>{e(i,r,s),t(i,r,s)}:n.contentQueries=e}function CT(n,e){let t=n.hostBindings;t?n.hostBindings=(i,r)=>{e(i,r),t(i,r)}:n.hostBindings=e}function C0(n){return DT(n)?Array.isArray(n)||!(n instanceof Map)&&Symbol.iterator in n:!1}function TT(n,e){if(Array.isArray(n))for(let t=0;t<n.length;t++)e(n[t]);else{let t=n[Symbol.iterator](),i;for(;!(i=t.next()).done;)e(i.value)}}function DT(n){return n!==null&&(typeof n=="function"||typeof n=="object")}function T0(n,e,t){return n[e]=t}function Tr(n,e,t){let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function D0(n,e,t,i){let r=Tr(n,e,t);return Tr(n,e+1,i)||r}function AT(n,e,t,i,r,s,o,a,l){let c=e.consts,u=Uh(e,n,4,o||null,a||null);Jv()&&g0(e,t,u,Xl(c,l),e0),u.mergedAttrs=$o(u.mergedAttrs,u.attrs),h_(e,u);let d=u.tView=Ch(2,u,i,r,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,c,null);return e.queries!==null&&(e.queries.template(e,u),d.queries=e.queries.embeddedTView(u)),u}function IT(n,e,t,i,r,s,o,a,l,c){let u=t+In,d=e.firstCreatePass?AT(u,e,n,i,r,s,o,a,l):e.data[u];ea(d,!1);let h=RT(e,n,d,t);yh()&&Oh(e,n,h,d),na(h,n);let f=h0(h,n,h,d);return n[u]=f,Dh(n,f),nT(f,d,n),lh(d)&&Ah(e,n,d),l!=null&&J_(n,d,c),d}function Rs(n,e,t,i,r,s,o,a){let l=ct(),c=en(),u=Xl(c.consts,s);return IT(l,c,n,e,t,i,r,u,o,a),Rs}var RT=NT;function NT(n,e,t,i){return vh(!0),e[Ut].createComment("")}var A0=new We("");var PT=(()=>{class n{static \u0275prov=mt({token:n,providedIn:"root",factory:()=>new qf})}return n})(),qf=class{queuedEffectCount=0;queues=new Map;schedule(e){this.enqueue(e)}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),this.queuedEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||(this.queuedEffectCount++,i.add(e))}flush(){for(;this.queuedEffectCount>0;)for(let[e,t]of this.queues)e===null?this.flushQueue(t):e.run(()=>this.flushQueue(t))}flushQueue(e){for(let t of e)e.delete(t),this.queuedEffectCount--,t.run()}};function ra(n){return!!n&&typeof n.then=="function"}function I0(n){return!!n&&typeof n.subscribe=="function"}var OT=new We("");var R0=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=lt(OT,{optional:!0})??[];injector=lt(xs);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=Uv(this.injector,r);if(ra(s))t.push(s);else if(I0(s)){let o=new Promise((a,l)=>{s.subscribe({complete:a,error:l})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=mt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),LT=new We("");function FT(){Od(()=>{throw new Pe(600,!1)})}function kT(n){return n.isBoundToModule}var UT=10;var Zo=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=lt(Zw);afterRenderManager=lt(dS);zonelessEnabled=lt(C_);rootEffectScheduler=lt(PT);dirtyFlags=0;tracingSnapshot=null;externalTestViews=new Set;afterTick=new Kn;get allViews(){return[...this.externalTestViews.keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];isStable=lt(pc).hasPendingTasks.pipe(mr(t=>!t));constructor(){lt(gc,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=lt(Xi);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=xs.NULL){vt(10);let s=t instanceof m0;if(!this._injector.get(R0).done){let f="";throw new Pe(405,f)}let a;s?a=t:a=this._injector.get(Vh).resolveComponentFactory(t),this.componentTypes.push(a.componentType);let l=kT(a)?void 0:this._injector.get(rc),c=i||a.selector,u=a.create(r,[],c,l),d=u.location.nativeElement,h=u.injector.get(A0,null);return h?.registerApplication(d),u.onDestroy(()=>{this.detachView(u.hostView),Ol(this.components,u),h?.unregisterApplication(d)}),this._loadComponent(u),vt(11,u),u}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){vt(12),this.tracingSnapshot!==null?this.tracingSnapshot.run(B_.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw new Pe(101,!1);let t=$e(null);try{this._runningTick=!0,this.synchronize()}catch(i){this.internalErrorHandler(i)}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,$e(t),this.afterTick.next(),vt(13)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ms,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<UT;)vt(14),this.synchronizeOnce(),vt(15)}synchronizeOnce(){if(this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush()),this.dirtyFlags&7){let t=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:i,notifyErrorHandler:r}of this.allViews)VT(i,r,t,this.zonelessEnabled);if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}else this._rendererFactory?.begin?.(),this._rendererFactory?.end?.();this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>fc(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;Ol(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView),this.tick(),this.components.push(t),this._injector.get(LT,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>Ol(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Pe(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=mt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Ol(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function VT(n,e,t,i){if(!t&&!fc(n))return;c0(n,e,t&&!i?0:1)}function BT(n,e,t,i){return Tr(n,hh(),t)?e+kl(t)+i:Ci}function HT(n,e,t,i,r,s){let o=vw(),a=D0(n,o,t,r);return i_(2),a?e+kl(t)+i+kl(r)+s:Ci}function Al(n,e){return n<<17|e<<2}function Dr(n){return n>>17&32767}function zT(n){return(n&2)==2}function GT(n,e){return n&131071|e<<17}function Xf(n){return n|2}function Es(n){return(n&131068)>>2}function of(n,e){return n&-131069|e<<2}function WT(n){return(n&1)===1}function Yf(n){return n|1}function jT(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=Dr(o),l=Es(o);n[i]=t;let c=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||Qo(d,u)>0)&&(c=!0)}else u=t;if(r)if(l!==0){let h=Dr(n[a+1]);n[i+1]=Al(h,a),h!==0&&(n[h+1]=of(n[h+1],i)),n[a+1]=GT(n[a+1],i)}else n[i+1]=Al(a,0),a!==0&&(n[a+1]=of(n[a+1],i)),a=i;else n[i+1]=Al(l,0),a===0?a=i:n[l+1]=of(n[l+1],i),l=i;c&&(n[i+1]=Xf(n[i+1])),fv(n,u,i,!0),fv(n,u,i,!1),$T(e,u,n,i,s),o=Al(a,l),s?e.classBindings=o:e.styleBindings=o}function $T(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&Qo(s,e)>=0&&(t[i+1]=Yf(t[i+1]))}function fv(n,e,t,i){let r=n[t+1],s=e===null,o=i?Dr(r):Es(r),a=!1;for(;o!==0&&(a===!1||s);){let l=n[o],c=n[o+1];qT(l,e)&&(a=!0,n[o+1]=i?Yf(c):Xf(c)),o=i?Dr(c):Es(c)}a&&(n[t+1]=i?Xf(r):Yf(r))}function qT(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Qo(n,e)>=0:!1}function Di(n,e,t){let i=ct(),r=hh();if(Tr(i,r,e)){let s=en(),o=u_();Q_(s,o,i,n,e,i[Ut],t,!1)}return Di}function hv(n,e,t,i,r){Ih(e,n,t,r?"class":"style",i)}function xc(n,e,t){return N0(n,e,t,!1),xc}function Hh(n,e){return N0(n,e,null,!0),Hh}function N0(n,e,t,i){let r=ct(),s=en(),o=i_(2);if(s.firstUpdatePass&&YT(s,n,o,i),e!==Ci&&Tr(r,o,e)){let a=s.data[Ar()];eD(s,a,r,r[Ut],n,r[o+1]=tD(e,t),i,o)}}function XT(n,e){return e>=n.expandoStartIndex}function YT(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[Ar()],o=XT(n,t);nD(s,i)&&e===null&&!o&&(e=!1),e=ZT(r,s,e,i),jT(r,s,e,t,o,i)}}function ZT(n,e,t,i){let r=bw(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=af(null,n,e,t,i),t=Ko(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=af(r,n,e,t,i),s===null){let l=KT(n,e,i);l!==void 0&&Array.isArray(l)&&(l=af(null,n,e,l[1],i),l=Ko(l,e.attrs,i),JT(n,e,i,l))}else s=QT(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function KT(n,e,t){let i=t?e.classBindings:e.styleBindings;if(Es(i)!==0)return n[Dr(i)]}function JT(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[Dr(r)]=i}function QT(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=Ko(i,o,t)}return Ko(i,e.attrs,t)}function af(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=Ko(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function Ko(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),kb(n,o,t?!0:e[++s]))}return n===void 0?null:n}function eD(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let l=n.data,c=l[a+1],u=WT(c)?pv(l,e,t,r,Es(c),o):void 0;if(!oc(u)){oc(s)||zT(c)&&(s=pv(l,null,t,r,a,o));let d=$v(Ar(),t);lC(i,o,d,r,s)}}function pv(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let l=n[r],c=Array.isArray(l),u=c?l[1]:l,d=u===null,h=t[r+1];h===Ci&&(h=d?Cn:void 0);let f=d?Yd(h,i):u===i?h:void 0;if(c&&!oc(f)&&(f=Yd(l,i)),oc(f)&&(a=f,o))return a;let m=n[r+1];r=o?Dr(m):Es(m)}if(e!==null){let l=s?e.residualClasses:e.residualStyles;l!=null&&(a=Yd(l,i))}return a}function oc(n){return n!==void 0}function tD(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=Dn(mS(n)))),n}function nD(n,e){return(n.flags&(e?8:16))!==0}function je(n,e,t,i){let r=ct(),s=en(),o=In+n,a=r[Ut],l=s.firstCreatePass?v0(o,s,r,e,e0,Jv(),t,i):s.data[o],c=iD(s,r,l,a,e,n);r[o]=c;let u=lh(l);return ea(l,!0),$_(a,c,l),!t0(l)&&yh()&&Oh(s,r,c,l),(uw()===0||u)&&na(c,r),dw(),u&&(Ah(s,r,l),G_(s,l,r)),i!==null&&J_(r,l),je}function He(){let n=Rn();e_()?yw():(n=n.parent,ea(n,!1));let e=n;pw(e)&&mw(),fw();let t=en();return t.firstCreatePass&&_0(t,e),e.classesWithoutHost!=null&&Dw(e)&&hv(t,e,ct(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&Aw(e)&&hv(t,e,ct(),e.stylesWithoutHost,!1),He}function Ai(n,e,t,i){return je(n,e,t,i),He(),Ai}var iD=(n,e,t,i,r,s)=>(vh(!0),W_(i,r,d_()));function Mc(){return ct()}var gr=void 0;function rD(n){let e=Math.floor(Math.abs(n)),t=n.toString().replace(/^[^.]*\.?/,"").length;return e===1&&t===0?1:5}var sD=["en",[["a","p"],["AM","PM"],gr],[["AM","PM"],gr,gr],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],gr,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],gr,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm a","h:mm:ss a","h:mm:ss a z","h:mm:ss a zzzz"],["{1}, {0}",gr,"{1} 'at' {0}",gr],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",rD],lf={};function zh(n){let e=oD(n),t=mv(e);if(t)return t;let i=e.split("-")[0];if(t=mv(i),t)return t;if(i==="en")return sD;throw new Pe(701,!1)}function mv(n){return n in lf||(lf[n]=Vo.ng&&Vo.ng.common&&Vo.ng.common.locales&&Vo.ng.common.locales[n]),lf[n]}var Ns=(function(n){return n[n.LocaleId=0]="LocaleId",n[n.DayPeriodsFormat=1]="DayPeriodsFormat",n[n.DayPeriodsStandalone=2]="DayPeriodsStandalone",n[n.DaysFormat=3]="DaysFormat",n[n.DaysStandalone=4]="DaysStandalone",n[n.MonthsFormat=5]="MonthsFormat",n[n.MonthsStandalone=6]="MonthsStandalone",n[n.Eras=7]="Eras",n[n.FirstDayOfWeek=8]="FirstDayOfWeek",n[n.WeekendRange=9]="WeekendRange",n[n.DateFormat=10]="DateFormat",n[n.TimeFormat=11]="TimeFormat",n[n.DateTimeFormat=12]="DateTimeFormat",n[n.NumberSymbols=13]="NumberSymbols",n[n.NumberFormats=14]="NumberFormats",n[n.CurrencyCode=15]="CurrencyCode",n[n.CurrencySymbol=16]="CurrencySymbol",n[n.CurrencyName=17]="CurrencyName",n[n.Currencies=18]="Currencies",n[n.Directionality=19]="Directionality",n[n.PluralCase=20]="PluralCase",n[n.ExtraData=21]="ExtraData",n})(Ns||{});function oD(n){return n.toLowerCase().replace(/_/g,"-")}var ac="en-US";var aD=ac;function lD(n){typeof n=="string"&&(aD=n.toLowerCase().replace(/_/g,"-"))}function gv(n,e,t){return function i(r){if(r===Function)return t;let s=Ss(n)?ii(n.index,e):e;Fh(s,5);let o=e[ln],a=yv(e,o,t,r),l=i.__ngNextListenerFn__;for(;l;)a=yv(e,o,l,r)&&a,l=l.__ngNextListenerFn__;return a}}function yv(n,e,t,i){let r=$e(null);try{return vt(6,e,t),t(i)!==!1}catch(s){return cD(n,s),!1}finally{vt(7,e,t),$e(r)}}function cD(n,e){let t=n[vs],i=t?t.get(Mi,null):null;i&&i.handleError(e)}function vv(n,e,t,i,r,s){let o=e[t],a=e[Ve],c=a.data[t].outputs[i],u=o[c],d=a.firstCreatePass?fh(a):null,h=dh(e),f=u.subscribe(s),m=h.length;h.push(s,f),d&&d.push(r,n.index,m,-(m+1))}function mn(n,e,t,i){let r=ct(),s=en(),o=Rn();return P0(s,r,r[Ut],o,n,e,i),mn}function uD(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[Wl],l=r[s+2];return a.length>l?a[l]:null}typeof o=="string"&&(s+=2)}return null}function P0(n,e,t,i,r,s,o){let a=lh(i),c=n.firstCreatePass?fh(n):null,u=dh(e),d=!0;if(i.type&3||o){let h=Si(i,e),f=o?o(h):h,m=u.length,v=o?p=>o(ni(p[i.index])):i.index,g=null;if(!o&&a&&(g=uD(n,e,r,i.index)),g!==null){let p=g.__ngLastListenerFn__||g;p.__ngNextListenerFn__=s,g.__ngLastListenerFn__=s,d=!1}else{s=gv(i,e,s),hS(e,f,r,s);let p=t.listen(f,r,s);u.push(s,p),c&&c.push(r,v,m,m+1)}}else s=gv(i,e,s);if(d){let h=i.outputs?.[r],f=i.hostDirectiveOutputs?.[r];if(f&&f.length)for(let m=0;m<f.length;m+=2){let v=f[m],g=f[m+1];vv(i,e,v,g,r,s)}if(h&&h.length)for(let m of h)vv(i,e,m,r,r,s)}}function Ir(n=1){return Sw(n)}function Gh(n,e,t){uT(n,e,t)}function Wh(n){let e=ct(),t=en(),i=r_();ph(i+1);let r=Bh(t,i);if(n.dirty&&sw(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=hT(e,i);n.reset(s,Jw),n.notifyOnChanges()}return!0}return!1}function jh(){return lT(ct(),r_())}function qe(n,e=""){let t=ct(),i=en(),r=n+In,s=i.firstCreatePass?Uh(i,r,1,e,null):i.data[r],o=dD(i,t,s,e,n);t[r]=o,yh()&&Oh(i,t,o,s),ea(s,!1)}var dD=(n,e,t,i,r)=>(vh(!0),gS(e[Ut],i));function Ki(n){return Rr("",n,""),Ki}function Rr(n,e,t){let i=ct(),r=BT(i,n,e,t);return r!==Ci&&O0(i,Ar(),r),Rr}function $h(n,e,t,i,r){let s=ct(),o=HT(s,n,e,t,i,r);return o!==Ci&&O0(s,Ar(),o),$h}function O0(n,e,t){let i=$v(e,n);yS(n[Ut],i,t)}function sa(n,e,t){R_(e)&&(e=e());let i=ct(),r=hh();if(Tr(i,r,e)){let s=en(),o=u_();Q_(s,o,i,n,e,i[Ut],t,!1)}return sa}function Ec(n,e){let t=R_(n);return t&&n.set(e),t}function oa(n,e){let t=ct(),i=en(),r=Rn();return P0(i,t,t[Ut],r,n,e),oa}function fD(n,e,t){let i=en();if(i.firstCreatePass){let r=ti(n);Zf(t,i.data,i.blueprint,r,!0),Zf(e,i.data,i.blueprint,r,!1)}}function Zf(n,e,t,i,r){if(n=Qt(n),Array.isArray(n))for(let s=0;s<n.length;s++)Zf(n[s],e,t,i,r);else{let s=en(),o=ct(),a=Rn(),l=ys(n)?n:Qt(n.provide),c=kv(n),u=a.providerIndexes&1048575,d=a.directiveStart,h=a.providerIndexes>>20;if(ys(n)||!n.multi){let f=new Sr(c,r,Je),m=uf(l,e,r?u:u+h,d);m===-1?(Ef(Jl(a,o),s,l),cf(s,n,e.length),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(f),o.push(f)):(t[m]=f,o[m]=f)}else{let f=uf(l,e,u+h,d),m=uf(l,e,u,u+h),v=f>=0&&t[f],g=m>=0&&t[m];if(r&&!g||!r&&!v){Ef(Jl(a,o),s,l);let p=mD(r?pD:hD,t.length,r,i,c);!r&&g&&(t[m].providerFactory=p),cf(s,n,e.length,0),e.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),o.push(p)}else{let p=L0(t[r?m:f],c,!r&&i);cf(s,n,f>-1?f:m,p)}!r&&i&&g&&t[m].componentProviders++}}}function cf(n,e,t,i){let r=ys(e),s=jb(e);if(r||s){let l=(s?Qt(e.useClass):e).prototype.ngOnDestroy;if(l){let c=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=c.indexOf(t);u===-1?c.push(t,[i,l]):c[u+1].push(i,l)}else c.push(t,l)}}}function L0(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function uf(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function hD(n,e,t,i,r){return Kf(this.multi,[])}function pD(n,e,t,i,r){let s=this.multi,o;if(this.providerFactory){let a=this.providerFactory.componentProviders,l=qo(i,i[Ve],this.providerFactory.index,r);o=l.slice(0,a),Kf(s,o);for(let c=a;c<l.length;c++)o.push(l[c])}else o=[],Kf(s,o);return o}function Kf(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function mD(n,e,t,i,r){let s=new Sr(n,t,Je);return s.multi=[],s.index=e,s.componentProviders=0,L0(s,r,i&&!t),s}function Ps(n,e=[]){return t=>{t.providersResolver=(i,r)=>fD(i,r?r(n):n,e)}}function F0(n,e){let t=n[e];return t===Ci?void 0:t}function gD(n,e,t,i,r,s){let o=e+t;return Tr(n,o,r)?T0(n,o+1,s?i.call(s,r):i(r)):F0(n,o+1)}function yD(n,e,t,i,r,s,o){let a=e+t;return D0(n,a,r,s)?T0(n,a+2,o?i.call(o,r,s):i(r,s)):F0(n,a+2)}function Nr(n,e){let t=en(),i,r=n+In;t.firstCreatePass?(i=vD(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let s=i.factory||(i.factory=_r(i.type,!0)),o,a=an(Je);try{let l=Kl(!1),c=s();return Kl(l),rw(t,ct(),r,c),c}finally{an(a)}}function vD(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function k0(n,e,t){let i=n+In,r=ct(),s=Xv(r,i);return U0(r,i)?gD(r,n_(),e,s.transform,t,s):s.transform(t)}function Os(n,e,t,i){let r=n+In,s=ct(),o=Xv(s,r);return U0(s,r)?yD(s,n_(),e,o.transform,t,i,o):o.transform(t,i)}function U0(n,e){return n[Ve].data[e].pure}var _D=(()=>{class n{zone=lt(Ht);changeDetectionScheduler=lt(Xo);applicationRef=lt(Zo);_onMicrotaskEmptySubscription;initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{this.applicationRef.tick()})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=mt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function xD({ngZoneFactory:n,ignoreChangesOutsideZone:e,scheduleInRootZone:t}){return n??=()=>new Ht(St(pt({},MD()),{scheduleInRootZone:t})),[{provide:Ht,useFactory:n},{provide:Hl,multi:!0,useFactory:()=>{let i=lt(_D,{optional:!0});return()=>i.initialize()}},{provide:Hl,multi:!0,useFactory:()=>{let i=lt(ED);return()=>{i.initialize()}}},e===!0?{provide:T_,useValue:!0}:[],{provide:D_,useValue:t??w_}]}function MD(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var ED=(()=>{class n{subscription=new Jt;initialized=!1;zone=lt(Ht);pendingTasks=lt(pc);initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{Ht.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{Ht.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=mt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var bD=(()=>{class n{appRef=lt(Zo);taskService=lt(pc);ngZone=lt(Ht);zonelessEnabled=lt(C_);tracing=lt(gc,{optional:!0});disableScheduling=lt(T_,{optional:!0})??!1;zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new Jt;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(ec):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(lt(D_,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{this.runningTick||this.cleanup()})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()})),this.disableScheduling||=!this.zonelessEnabled&&(this.ngZone instanceof Tf||!this.zoneIsDefined)}notify(t){if(!this.zonelessEnabled&&t===5)return;let i=!1;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2,i=!0;break}case 12:{this.appRef.dirtyFlags|=16,i=!0;break}case 13:{this.appRef.dirtyFlags|=2,i=!0;break}case 11:{i=!0;break}default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick(i))return;let r=this.useMicrotaskScheduler?Zy:A_;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(t){return!(this.disableScheduling&&!t||this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(ec+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){throw this.taskService.remove(t),i}finally{this.cleanup()}this.useMicrotaskScheduler=!0,Zy(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=mt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function wD(){return typeof $localize<"u"&&$localize.locale||ac}var bc=new We("",{providedIn:"root",factory:()=>lt(bc,Ge.Optional|Ge.SkipSelf)||wD()});var Jf=new We(""),SD=new We("");function Uo(n){return!n.moduleRef}function CD(n){let e=Uo(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Ht);return t.run(()=>{Uo(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(Mi,null),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:s=>{i.handleError(s)}})}),Uo(n)){let s=()=>e.destroy(),o=n.platformInjector.get(Jf);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(Jf);o.add(s),n.moduleRef.onDestroy(()=>{Ol(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return DD(i,t,()=>{let s=e.get(R0);return s.runInitializers(),s.donePromise.then(()=>{let o=e.get(bc,ac);if(lD(o||ac),!e.get(SD,!0))return Uo(n)?e.get(Zo):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Uo(n)){let l=e.get(Zo);return n.rootComponent!==void 0&&l.bootstrap(n.rootComponent),l}else return TD(n.moduleRef,n.allPlatformModules),n.moduleRef})})})}function TD(n,e){let t=n.injector.get(Zo);if(n._bootstrapComponents.length>0)n._bootstrapComponents.forEach(i=>t.bootstrap(i));else if(n.instance.ngDoBootstrap)n.instance.ngDoBootstrap(t);else throw new Pe(-403,!1);e.push(n)}function DD(n,e,t){try{let i=t();return ra(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n.handleError(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n.handleError(i)),i}}var Ll=null;function AD(n=[],e){return xs.create({name:e,providers:[{provide:uc,useValue:"platform"},{provide:Jf,useValue:new Set([()=>Ll=null])},...n]})}function ID(n=[]){if(Ll)return Ll;let e=AD(n);return Ll=e,FT(),RD(e),e}function RD(n){let e=n.get(bh,null);Uv(n,()=>{e?.forEach(t=>t())})}var qh=(()=>{class n{static __NG_ELEMENT_ID__=ND}return n})();function ND(n){return PD(Rn(),ct(),(n&16)===16)}function PD(n,e,t){if(Ss(n)&&!t){let i=ii(n.index,e);return new Yo(i,i)}else if(n.type&175){let i=e[ei];return new Yo(i,e)}return null}var Qf=class{constructor(){}supports(e){return C0(e)}create(e){return new eh(e)}},OD=(n,e)=>e,eh=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(e){this._trackByFn=e||OD}forEachItem(e){let t;for(t=this._itHead;t!==null;t=t._next)e(t)}forEachOperation(e){let t=this._itHead,i=this._removalsHead,r=0,s=null;for(;t||i;){let o=!i||t&&t.currentIndex<_v(i,r,s)?t:i,a=_v(o,r,s),l=o.currentIndex;if(o===i)r--,i=i._nextRemoved;else if(t=t._next,o.previousIndex==null)r++;else{s||(s=[]);let c=a-r,u=l-r;if(c!=u){for(let h=0;h<c;h++){let f=h<s.length?s[h]:s[h]=0,m=f+h;u<=m&&m<c&&(s[h]=f+1)}let d=o.previousIndex;s[d]=u-c}}a!==l&&e(o,a,l)}}forEachPreviousItem(e){let t;for(t=this._previousItHead;t!==null;t=t._nextPrevious)e(t)}forEachAddedItem(e){let t;for(t=this._additionsHead;t!==null;t=t._nextAdded)e(t)}forEachMovedItem(e){let t;for(t=this._movesHead;t!==null;t=t._nextMoved)e(t)}forEachRemovedItem(e){let t;for(t=this._removalsHead;t!==null;t=t._nextRemoved)e(t)}forEachIdentityChange(e){let t;for(t=this._identityChangesHead;t!==null;t=t._nextIdentityChange)e(t)}diff(e){if(e==null&&(e=[]),!C0(e))throw new Pe(900,!1);return this.check(e)?this:null}onDestroy(){}check(e){this._reset();let t=this._itHead,i=!1,r,s,o;if(Array.isArray(e)){this.length=e.length;for(let a=0;a<this.length;a++)s=e[a],o=this._trackByFn(a,s),t===null||!Object.is(t.trackById,o)?(t=this._mismatch(t,s,o,a),i=!0):(i&&(t=this._verifyReinsertion(t,s,o,a)),Object.is(t.item,s)||this._addIdentityChange(t,s)),t=t._next}else r=0,TT(e,a=>{o=this._trackByFn(r,a),t===null||!Object.is(t.trackById,o)?(t=this._mismatch(t,a,o,r),i=!0):(i&&(t=this._verifyReinsertion(t,a,o,r)),Object.is(t.item,a)||this._addIdentityChange(t,a)),t=t._next,r++}),this.length=r;return this._truncate(t),this.collection=e,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let e;for(e=this._previousItHead=this._itHead;e!==null;e=e._next)e._nextPrevious=e._next;for(e=this._additionsHead;e!==null;e=e._nextAdded)e.previousIndex=e.currentIndex;for(this._additionsHead=this._additionsTail=null,e=this._movesHead;e!==null;e=e._nextMoved)e.previousIndex=e.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(e,t,i,r){let s;return e===null?s=this._itTail:(s=e._prev,this._remove(e)),e=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),e!==null?(Object.is(e.item,t)||this._addIdentityChange(e,t),this._reinsertAfter(e,s,r)):(e=this._linkedRecords===null?null:this._linkedRecords.get(i,r),e!==null?(Object.is(e.item,t)||this._addIdentityChange(e,t),this._moveAfter(e,s,r)):e=this._addAfter(new th(t,i),s,r)),e}_verifyReinsertion(e,t,i,r){let s=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return s!==null?e=this._reinsertAfter(s,e._prev,r):e.currentIndex!=r&&(e.currentIndex=r,this._addToMoves(e,r)),e}_truncate(e){for(;e!==null;){let t=e._next;this._addToRemovals(this._unlink(e)),e=t}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(e,t,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(e);let r=e._prevRemoved,s=e._nextRemoved;return r===null?this._removalsHead=s:r._nextRemoved=s,s===null?this._removalsTail=r:s._prevRemoved=r,this._insertAfter(e,t,i),this._addToMoves(e,i),e}_moveAfter(e,t,i){return this._unlink(e),this._insertAfter(e,t,i),this._addToMoves(e,i),e}_addAfter(e,t,i){return this._insertAfter(e,t,i),this._additionsTail===null?this._additionsTail=this._additionsHead=e:this._additionsTail=this._additionsTail._nextAdded=e,e}_insertAfter(e,t,i){let r=t===null?this._itHead:t._next;return e._next=r,e._prev=t,r===null?this._itTail=e:r._prev=e,t===null?this._itHead=e:t._next=e,this._linkedRecords===null&&(this._linkedRecords=new lc),this._linkedRecords.put(e),e.currentIndex=i,e}_remove(e){return this._addToRemovals(this._unlink(e))}_unlink(e){this._linkedRecords!==null&&this._linkedRecords.remove(e);let t=e._prev,i=e._next;return t===null?this._itHead=i:t._next=i,i===null?this._itTail=t:i._prev=t,e}_addToMoves(e,t){return e.previousIndex===t||(this._movesTail===null?this._movesTail=this._movesHead=e:this._movesTail=this._movesTail._nextMoved=e),e}_addToRemovals(e){return this._unlinkedRecords===null&&(this._unlinkedRecords=new lc),this._unlinkedRecords.put(e),e.currentIndex=null,e._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=e,e._prevRemoved=null):(e._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=e),e}_addIdentityChange(e,t){return e.item=t,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=e:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=e,e}},th=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(e,t){this.item=e,this.trackById=t}},nh=class{_head=null;_tail=null;add(e){this._head===null?(this._head=this._tail=e,e._nextDup=null,e._prevDup=null):(this._tail._nextDup=e,e._prevDup=this._tail,e._nextDup=null,this._tail=e)}get(e,t){let i;for(i=this._head;i!==null;i=i._nextDup)if((t===null||t<=i.currentIndex)&&Object.is(i.trackById,e))return i;return null}remove(e){let t=e._prevDup,i=e._nextDup;return t===null?this._head=i:t._nextDup=i,i===null?this._tail=t:i._prevDup=t,this._head===null}},lc=class{map=new Map;put(e){let t=e.trackById,i=this.map.get(t);i||(i=new nh,this.map.set(t,i)),i.add(e)}get(e,t){let i=e,r=this.map.get(i);return r?r.get(e,t):null}remove(e){let t=e.trackById;return this.map.get(t).remove(e)&&this.map.delete(t),e}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function _v(n,e,t){let i=n.previousIndex;if(i===null)return i;let r=0;return t&&i<t.length&&(r=t[i]),i+e+r}function xv(){return new Xh([new Qf])}var Xh=(()=>{class n{factories;static \u0275prov=mt({token:n,providedIn:"root",factory:xv});constructor(t){this.factories=t}static create(t,i){if(i!=null){let r=i.factories.slice();t=t.concat(r)}return new n(t)}static extend(t){return{provide:n,useFactory:i=>n.create(t,i||xv()),deps:[[n,new Pb,new Nb]]}}find(t){let i=this.factories.find(r=>r.supports(t));if(i!=null)return i;throw new Pe(901,!1)}}return n})();function V0(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;vt(8);try{let s=r?.injector??ID(i),o=[xD({}),{provide:Xo,useExisting:bD},...t||[]],a=new sc({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return CD({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{vt(9)}}function B0(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function Ji(n){return Ud(n)}function wc(n,e){return Pd(n,e?.equal)}var Mv=class{[Fn];constructor(e){this[Fn]=e}destroy(){this[Fn].destroy()}};var li=new We("");var H0=null;function er(){return H0}function Yh(n){H0??=n}var aa=class{};var Jh=(function(n){return n[n.Decimal=0]="Decimal",n[n.Percent=1]="Percent",n[n.Currency=2]="Currency",n[n.Scientific=3]="Scientific",n})(Jh||{});var ci={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function Ls(n,e){let t=zh(n),i=t[Ns.NumberSymbols][e];if(typeof i>"u"){if(e===ci.CurrencyDecimal)return t[Ns.NumberSymbols][ci.Decimal];if(e===ci.CurrencyGroup)return t[Ns.NumberSymbols][ci.Group]}return i}function j0(n,e){return zh(n)[Ns.NumberFormats][e]}var LD=/^(\d+)?\.((\d+)(-(\d+))?)?$/,z0=22,Sc=".",la="0",FD=";",kD=",",Zh="#";function UD(n,e,t,i,r,s,o=!1){let a="",l=!1;if(!isFinite(n))a=Ls(t,ci.Infinity);else{let c=HD(n);o&&(c=BD(c));let u=e.minInt,d=e.minFrac,h=e.maxFrac;if(s){let M=s.match(LD);if(M===null)throw new Error(`${s} is not a valid digit info`);let _=M[1],y=M[3],A=M[5];_!=null&&(u=Kh(_)),y!=null&&(d=Kh(y)),A!=null?h=Kh(A):y!=null&&d>h&&(h=d);let C=100;if(u>C||d>C||h>C)throw new Pe(2306,!1)}zD(c,d,h);let f=c.digits,m=c.integerLen,v=c.exponent,g=[];for(l=f.every(M=>!M);m<u;m++)f.unshift(0);for(;m<0;m++)f.unshift(0);m>0?g=f.splice(m,f.length):(g=f,f=[0]);let p=[];for(f.length>=e.lgSize&&p.unshift(f.splice(-e.lgSize,f.length).join(""));f.length>e.gSize;)p.unshift(f.splice(-e.gSize,f.length).join(""));f.length&&p.unshift(f.join("")),a=p.join(Ls(t,i)),g.length&&(a+=Ls(t,r)+g.join("")),v&&(a+=Ls(t,ci.Exponential)+"+"+v)}return n<0&&!l?a=e.negPre+a+e.negSuf:a=e.posPre+a+e.posSuf,a}function $0(n,e,t){let i=j0(e,Jh.Decimal),r=VD(i,Ls(e,ci.MinusSign));return UD(n,r,e,ci.Group,ci.Decimal,t)}function VD(n,e="-"){let t={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=n.split(FD),r=i[0],s=i[1],o=r.indexOf(Sc)!==-1?r.split(Sc):[r.substring(0,r.lastIndexOf(la)+1),r.substring(r.lastIndexOf(la)+1)],a=o[0],l=o[1]||"";t.posPre=a.substring(0,a.indexOf(Zh));for(let u=0;u<l.length;u++){let d=l.charAt(u);d===la?t.minFrac=t.maxFrac=u+1:d===Zh?t.maxFrac=u+1:t.posSuf+=d}let c=a.split(kD);if(t.gSize=c[1]?c[1].length:0,t.lgSize=c[2]||c[1]?(c[2]||c[1]).length:0,s){let u=r.length-t.posPre.length-t.posSuf.length,d=s.indexOf(Zh);t.negPre=s.substring(0,d).replace(/'/g,""),t.negSuf=s.slice(d+u).replace(/'/g,"")}else t.negPre=e+t.posPre,t.negSuf=t.posSuf;return t}function BD(n){if(n.digits[0]===0)return n;let e=n.digits.length-n.integerLen;return n.exponent?n.exponent+=2:(e===0?n.digits.push(0,0):e===1&&n.digits.push(0),n.integerLen+=2),n}function HD(n){let e=Math.abs(n)+"",t=0,i,r,s,o,a;for((r=e.indexOf(Sc))>-1&&(e=e.replace(Sc,"")),(s=e.search(/e/i))>0?(r<0&&(r=s),r+=+e.slice(s+1),e=e.substring(0,s)):r<0&&(r=e.length),s=0;e.charAt(s)===la;s++);if(s===(a=e.length))i=[0],r=1;else{for(a--;e.charAt(a)===la;)a--;for(r-=s,i=[],o=0;s<=a;s++,o++)i[o]=Number(e.charAt(s))}return r>z0&&(i=i.splice(0,z0-1),t=r-1,r=1),{digits:i,exponent:t,integerLen:r}}function zD(n,e,t){if(e>t)throw new Error(`The minimum number of digits after fraction (${e}) is higher than the maximum (${t}).`);let i=n.digits,r=i.length-n.integerLen,s=Math.min(Math.max(e,r),t),o=s+n.integerLen,a=i[o];if(o>0){i.splice(Math.max(n.integerLen,o));for(let d=o;d<i.length;d++)i[d]=0}else{r=Math.max(0,r),n.integerLen=1,i.length=Math.max(1,o=s+1),i[0]=0;for(let d=1;d<o;d++)i[d]=0}if(a>=5)if(o-1<0){for(let d=0;d>o;d--)i.unshift(0),n.integerLen++;i.unshift(1),n.integerLen++}else i[o-1]++;for(;r<Math.max(0,s);r++)i.push(0);let l=s!==0,c=e+n.integerLen,u=i.reduceRight(function(d,h,f,m){return h=h+d,m[f]=h<10?h:h-10,l&&(m[f]===0&&f>=c?m.pop():l=!1),h>=10?1:0},0);u&&(i.unshift(u),n.integerLen++)}function Kh(n){let e=parseInt(n);if(isNaN(e))throw new Error("Invalid integer literal when parsing "+n);return e}var Cc=class{$implicit;ngForOf;index;count;constructor(e,t,i,r){this.$implicit=e,this.ngForOf=t,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},Dc=(()=>{class n{_viewContainer;_template;_differs;set ngForOf(t){this._ngForOf=t,this._ngForOfDirty=!0}set ngForTrackBy(t){this._trackByFn=t}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(t,i,r){this._viewContainer=t,this._template=i,this._differs=r}set ngForTemplate(t){t&&(this._template=t)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let t=this._ngForOf;!this._differ&&t&&(this._differ=this._differs.find(t).create(this.ngForTrackBy))}if(this._differ){let t=this._differ.diff(this._ngForOf);t&&this._applyChanges(t)}}_applyChanges(t){let i=this._viewContainer;t.forEachOperation((r,s,o)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new Cc(r.item,this._ngForOf,-1,-1),o===null?void 0:o);else if(o==null)i.remove(s===null?void 0:s);else if(s!==null){let a=i.get(s);i.move(a,o),G0(a,r)}});for(let r=0,s=i.length;r<s;r++){let a=i.get(r).context;a.index=r,a.count=s,a.ngForOf=this._ngForOf}t.forEachIdentityChange(r=>{let s=i.get(r.currentIndex);G0(s,r)})}static ngTemplateContextGuard(t,i){return!0}static \u0275fac=function(i){return new(i||n)(Je(As),Je(Cr),Je(Xh))};static \u0275dir=cn({type:n,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return n})();function G0(n,e){n.context.$implicit=e.item}var Qh=(()=>{class n{_viewContainer;_context=new Tc;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(t,i){this._viewContainer=t,this._thenTemplateRef=i}set ngIf(t){this._context.$implicit=this._context.ngIf=t,this._updateView()}set ngIfThen(t){W0(t,!1),this._thenTemplateRef=t,this._thenViewRef=null,this._updateView()}set ngIfElse(t){W0(t,!1),this._elseTemplateRef=t,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(t,i){return!0}static \u0275fac=function(i){return new(i||n)(Je(As),Je(Cr))};static \u0275dir=cn({type:n,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return n})(),Tc=class{$implicit=null;ngIf=null};function W0(n,e){if(n&&!n.createEmbeddedView)throw new Pe(2020,!1)}function GD(n,e){return new Pe(2100,!1)}var ep=(()=>{class n{_locale;constructor(t){this._locale=t}transform(t,i,r){if(!WD(t))return null;r||=this._locale;try{let s=jD(t);return $0(s,r,i)}catch(s){throw GD(n,s.message)}}static \u0275fac=function(i){return new(i||n)(Je(bc,16))};static \u0275pipe=b0({name:"number",type:n,pure:!0})}return n})();function WD(n){return!(n==null||n===""||n!==n)}function jD(n){if(typeof n=="string"&&!isNaN(Number(n)-parseFloat(n)))return Number(n);if(typeof n!="number")throw new Error(`${n} is not a number`);return n}var Ac=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Is({type:n});static \u0275inj=bs({})}return n})();function tp(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var np="browser",q0="server";function Ic(n){return n===q0}var ca=class{};var Pc=new We(""),op=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(r=>{r.manager=this}),this._plugins=t.slice().reverse()}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Pe(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(ft(Pc),ft(Ht))};static \u0275prov=mt({token:n,factory:n.\u0275fac})}return n})(),ua=class{_doc;constructor(e){this._doc=e}manager},Rc="ng-app-id";function X0(n){for(let e of n)e.remove()}function Y0(n,e){let t=e.createElement("style");return t.textContent=n,t}function qD(n,e,t,i){let r=n.head?.querySelectorAll(`style[${Rc}="${e}"],link[${Rc}="${e}"]`);if(r)for(let s of r)s.removeAttribute(Rc),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function rp(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var ap=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;isServer;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,this.isServer=Ic(s),qD(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,Y0);i?.forEach(r=>this.addUsage(r,this.external,rp))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(X0(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])X0(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,Y0(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,rp(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),this.isServer&&i.setAttribute(Rc,this.appId),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(ft(li),ft(Eh),ft(wh,8),ft(ia))};static \u0275prov=mt({token:n,factory:n.\u0275fac})}return n})(),ip={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},lp=/%COMP%/g;var K0="%COMP%",XD=`_nghost-${K0}`,YD=`_ngcontent-${K0}`,ZD=!0,KD=new We("",{providedIn:"root",factory:()=>ZD});function JD(n){return YD.replace(lp,n)}function QD(n){return XD.replace(lp,n)}function J0(n,e){return e.map(t=>t.replace(lp,n))}var cp=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;platformId;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;platformIsServer;constructor(t,i,r,s,o,a,l,c=null,u=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.platformId=a,this.ngZone=l,this.nonce=c,this.tracingService=u,this.platformIsServer=Ic(a),this.defaultRenderer=new da(t,o,l,this.platformIsServer,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;this.platformIsServer&&i.encapsulation===ri.ShadowDom&&(i=St(pt({},i),{encapsulation:ri.Emulated}));let r=this.getOrCreateRenderer(t,i);return r instanceof Nc?r.applyToHost(t):r instanceof fa&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.platformIsServer,h=this.tracingService;switch(i.encapsulation){case ri.Emulated:s=new Nc(l,c,i,this.appId,u,o,a,d,h);break;case ri.ShadowDom:return new sp(l,c,t,i,o,a,this.nonce,d,h);default:s=new fa(l,c,i,u,o,a,d,h);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(ft(op),ft(ap),ft(Eh),ft(KD),ft(li),ft(ia),ft(Ht),ft(wh),ft(gc,8))};static \u0275prov=mt({token:n,factory:n.\u0275fac})}return n})(),da=class{eventManager;doc;ngZone;platformIsServer;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r,s){this.eventManager=e,this.doc=t,this.ngZone=i,this.platformIsServer=r,this.tracingService=s}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(ip[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(Z0(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(Z0(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Pe(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=ip[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=ip[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(Ei.DashCase|Ei.Important)?e.style.setProperty(t,i,r&Ei.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&Ei.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=er().getGlobalEventTarget(this.doc,e),!e))throw new Pe(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;(this.platformIsServer?this.ngZone.runGuarded(()=>e(t)):e(t))===!1&&t.preventDefault()}}};function Z0(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var sp=class extends da{sharedStylesHost;hostEl;shadowRoot;constructor(e,t,i,r,s,o,a,l,c){super(e,s,o,l,c),this.sharedStylesHost=t,this.hostEl=i,this.shadowRoot=i.attachShadow({mode:"open"}),this.sharedStylesHost.addHost(this.shadowRoot);let u=r.styles;u=J0(r.id,u);for(let h of u){let f=document.createElement("style");a&&f.setAttribute("nonce",a),f.textContent=h,this.shadowRoot.appendChild(f)}let d=r.getExternalStyles?.();if(d)for(let h of d){let f=rp(h,s);a&&f.setAttribute("nonce",a),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost.removeHost(this.shadowRoot)}},fa=class extends da{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,l,c){super(e,s,o,a,l),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let u=i.styles;this.styles=c?J0(c,u):u,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Nc=class extends fa{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,l,c){let u=r+"-"+i.id;super(e,t,i,s,o,a,l,c,u),this.contentAttr=JD(u),this.hostAttr=QD(u)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var Oc=class n extends aa{supportsDOMEvents=!0;static makeCurrent(){Yh(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=eA();return t==null?null:tA(t)}resetBaseElement(){ha=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return tp(document.cookie,e)}},ha=null;function eA(){return ha=ha||document.head.querySelector("base"),ha?ha.getAttribute("href"):null}function tA(n){return new URL(n,document.baseURI).pathname}var nA=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=mt({token:n,factory:n.\u0275fac})}return n})(),ex=(()=>{class n extends ua{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(ft(li))};static \u0275prov=mt({token:n,factory:n.\u0275fac})}return n})(),Q0=["alt","control","meta","shift"],iA={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},rA={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},tx=(()=>{class n extends ua{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>er().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),Q0.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),o+=c+".")}),o+=s,i.length!=0||s.length===0)return null;let l={};return l.domEventName=r,l.fullKey=o,l}static matchEventFullKeyCode(t,i){let r=iA[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Q0.forEach(o=>{if(o!==r){let a=rA[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(ft(li))};static \u0275prov=mt({token:n,factory:n.\u0275fac})}return n})();function up(n,e,t){return V0(pt({rootComponent:n,platformRef:t?.platformRef},sA(e)))}function sA(n){return{appProviders:[...uA,...n?.providers??[]],platformProviders:cA}}function oA(){Oc.makeCurrent()}function aA(){return new Mi}function lA(){return U_(document),document}var cA=[{provide:ia,useValue:np},{provide:bh,useValue:oA,multi:!0},{provide:li,useFactory:lA}];var uA=[{provide:uc,useValue:"root"},{provide:Mi,useFactory:aA},{provide:Pc,useClass:ex,multi:!0,deps:[li]},{provide:Pc,useClass:tx,multi:!0,deps:[li]},cp,ap,op,{provide:Ms,useExisting:cp},{provide:ca,useClass:nA},[]];var ux=(()=>{class n{_renderer;_elementRef;onChange=t=>{};onTouched=()=>{};constructor(t,i){this._renderer=t,this._elementRef=i}setProperty(t,i){this._renderer.setProperty(this._elementRef.nativeElement,t,i)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static \u0275fac=function(i){return new(i||n)(Je(Ds),Je(ai))};static \u0275dir=cn({type:n})}return n})(),Vc=(()=>{class n extends ux{static \u0275fac=(()=>{let t;return function(r){return(t||(t=ta(n)))(r||n)}})();static \u0275dir=cn({type:n,features:[Ti]})}return n})(),_a=new We("");var dA={provide:_a,useExisting:Zi(()=>Bc),multi:!0};function fA(){let n=er()?er().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var hA=new We(""),Bc=(()=>{class n extends ux{_compositionMode;_composing=!1;constructor(t,i,r){super(t,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!fA())}writeValue(t){let i=t??"";this.setProperty("value",i)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static \u0275fac=function(i){return new(i||n)(Je(Ds),Je(ai),Je(hA,8))};static \u0275dir=cn({type:n,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&mn("input",function(o){return r._handleInput(o.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(o){return r._compositionEnd(o.target.value)})},standalone:!1,features:[Ps([dA]),Ti]})}return n})();var pA=new We(""),mA=new We("");function dx(n){return n!=null}function fx(n){return ra(n)?qd(n):n}function hx(n){let e={};return n.forEach(t=>{e=t!=null?pt(pt({},e),t):e}),Object.keys(e).length===0?null:e}function px(n,e){return e.map(t=>t(n))}function gA(n){return!n.validate}function mx(n){return n.map(e=>gA(e)?e:t=>e.validate(t))}function yA(n){if(!n)return null;let e=n.filter(dx);return e.length==0?null:function(t){return hx(px(t,e))}}function gx(n){return n!=null?yA(mx(n)):null}function vA(n){if(!n)return null;let e=n.filter(dx);return e.length==0?null:function(t){let i=px(t,e).map(fx);return Xd(i).pipe(mr(hx))}}function yx(n){return n!=null?vA(mx(n)):null}function nx(n,e){return n===null?[e]:Array.isArray(n)?[...n,e]:[n,e]}function _A(n){return n._rawValidators}function xA(n){return n._rawAsyncValidators}function dp(n){return n?Array.isArray(n)?n:[n]:[]}function Fc(n,e){return Array.isArray(n)?n.includes(e):n===e}function ix(n,e){let t=dp(e);return dp(n).forEach(r=>{Fc(t,r)||t.push(r)}),t}function rx(n,e){return dp(e).filter(t=>!Fc(n,t))}var kc=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(e){this._rawValidators=e||[],this._composedValidatorFn=gx(this._rawValidators)}_setAsyncValidators(e){this._rawAsyncValidators=e||[],this._composedAsyncValidatorFn=yx(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(e){this._onDestroyCallbacks.push(e)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(e=>e()),this._onDestroyCallbacks=[]}reset(e=void 0){this.control&&this.control.reset(e)}hasError(e,t){return this.control?this.control.hasError(e,t):!1}getError(e,t){return this.control?this.control.getError(e,t):null}},fp=class extends kc{name;get formDirective(){return null}get path(){return null}},va=class extends kc{_parent=null;name=null;valueAccessor=null},hp=class{_cd;constructor(e){this._cd=e}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}},MA={"[class.ng-untouched]":"isUntouched","[class.ng-touched]":"isTouched","[class.ng-pristine]":"isPristine","[class.ng-dirty]":"isDirty","[class.ng-valid]":"isValid","[class.ng-invalid]":"isInvalid","[class.ng-pending]":"isPending"},qV=St(pt({},MA),{"[class.ng-submitted]":"isSubmitted"}),vx=(()=>{class n extends hp{constructor(t){super(t)}static \u0275fac=function(i){return new(i||n)(Je(va,2))};static \u0275dir=cn({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&Hh("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Ti]})}return n})();var pa="VALID",Lc="INVALID",Fs="PENDING",ma="DISABLED",Us=class{},Uc=class extends Us{value;source;constructor(e,t){super(),this.value=e,this.source=t}},ga=class extends Us{pristine;source;constructor(e,t){super(),this.pristine=e,this.source=t}},ya=class extends Us{touched;source;constructor(e,t){super(),this.touched=e,this.source=t}},ks=class extends Us{status;source;constructor(e,t){super(),this.status=e,this.source=t}};function EA(n){return(Hc(n)?n.validators:n)||null}function bA(n){return Array.isArray(n)?gx(n):n||null}function wA(n,e){return(Hc(e)?e.asyncValidators:n)||null}function SA(n){return Array.isArray(n)?yx(n):n||null}function Hc(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}var pp=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(e,t){this._assignValidators(e),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(e){this._rawValidators=this._composedValidatorFn=e}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(e){this._rawAsyncValidators=this._composedAsyncValidatorFn=e}get parent(){return this._parent}get status(){return Ji(this.statusReactive)}set status(e){Ji(()=>this.statusReactive.set(e))}_status=wc(()=>this.statusReactive());statusReactive=mc(void 0);get valid(){return this.status===pa}get invalid(){return this.status===Lc}get pending(){return this.status==Fs}get disabled(){return this.status===ma}get enabled(){return this.status!==ma}errors;get pristine(){return Ji(this.pristineReactive)}set pristine(e){Ji(()=>this.pristineReactive.set(e))}_pristine=wc(()=>this.pristineReactive());pristineReactive=mc(!0);get dirty(){return!this.pristine}get touched(){return Ji(this.touchedReactive)}set touched(e){Ji(()=>this.touchedReactive.set(e))}_touched=wc(()=>this.touchedReactive());touchedReactive=mc(!1);get untouched(){return!this.touched}_events=new Kn;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(e){this._assignValidators(e)}setAsyncValidators(e){this._assignAsyncValidators(e)}addValidators(e){this.setValidators(ix(e,this._rawValidators))}addAsyncValidators(e){this.setAsyncValidators(ix(e,this._rawAsyncValidators))}removeValidators(e){this.setValidators(rx(e,this._rawValidators))}removeAsyncValidators(e){this.setAsyncValidators(rx(e,this._rawAsyncValidators))}hasValidator(e){return Fc(this._rawValidators,e)}hasAsyncValidator(e){return Fc(this._rawAsyncValidators,e)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(e={}){let t=this.touched===!1;this.touched=!0;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsTouched(St(pt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new ya(!0,i))}markAllAsTouched(e={}){this.markAsTouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(e))}markAsUntouched(e={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:e.emitEvent,sourceControl:i})}),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,i),t&&e.emitEvent!==!1&&this._events.next(new ya(!1,i))}markAsDirty(e={}){let t=this.pristine===!0;this.pristine=!1;let i=e.sourceControl??this;this._parent&&!e.onlySelf&&this._parent.markAsDirty(St(pt({},e),{sourceControl:i})),t&&e.emitEvent!==!1&&this._events.next(new ga(!1,i))}markAsPristine(e={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=e.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:e.emitEvent})}),this._parent&&!e.onlySelf&&this._parent._updatePristine(e,i),t&&e.emitEvent!==!1&&this._events.next(new ga(!0,i))}markAsPending(e={}){this.status=Fs;let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new ks(this.status,t)),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.markAsPending(St(pt({},e),{sourceControl:t}))}disable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=ma,this.errors=null,this._forEachChild(r=>{r.disable(St(pt({},e),{onlySelf:!0}))}),this._updateValue();let i=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new Uc(this.value,i)),this._events.next(new ks(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(St(pt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(e={}){let t=this._parentMarkedDirty(e.onlySelf);this.status=pa,this._forEachChild(i=>{i.enable(St(pt({},e),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent}),this._updateAncestors(St(pt({},e),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(e,t){this._parent&&!e.onlySelf&&(this._parent.updateValueAndValidity(e),e.skipPristineCheck||this._parent._updatePristine({},t),this._parent._updateTouched({},t))}setParent(e){this._parent=e}getRawValue(){return this.value}updateValueAndValidity(e={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===pa||this.status===Fs)&&this._runAsyncValidator(i,e.emitEvent)}let t=e.sourceControl??this;e.emitEvent!==!1&&(this._events.next(new Uc(this.value,t)),this._events.next(new ks(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._parent&&!e.onlySelf&&this._parent.updateValueAndValidity(St(pt({},e),{sourceControl:t}))}_updateTreeValidity(e={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(e)),this.updateValueAndValidity({onlySelf:!0,emitEvent:e.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?ma:pa}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(e,t){if(this.asyncValidator){this.status=Fs,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1};let i=fx(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:t,shouldHaveEmitted:e})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let e=this._hasOwnPendingAsyncValidator?.emitEvent??!1;return this._hasOwnPendingAsyncValidator=null,e}return!1}setErrors(e,t={}){this.errors=e,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(e){let t=e;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((i,r)=>i&&i._find(r),this)}getError(e,t){let i=t?this.get(t):this;return i&&i.errors?i.errors[e]:null}hasError(e,t){return!!this.getError(e,t)}get root(){let e=this;for(;e._parent;)e=e._parent;return e}_updateControlsErrors(e,t,i){this.status=this._calculateStatus(),e&&this.statusChanges.emit(this.status),(e||i)&&this._events.next(new ks(this.status,t)),this._parent&&this._parent._updateControlsErrors(e,t,i)}_initObservables(){this.valueChanges=new Tn,this.statusChanges=new Tn}_calculateStatus(){return this._allControlsDisabled()?ma:this.errors?Lc:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Fs)?Fs:this._anyControlsHaveStatus(Lc)?Lc:pa}_anyControlsHaveStatus(e){return this._anyControls(t=>t.status===e)}_anyControlsDirty(){return this._anyControls(e=>e.dirty)}_anyControlsTouched(){return this._anyControls(e=>e.touched)}_updatePristine(e,t){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,this._parent&&!e.onlySelf&&this._parent._updatePristine(e,t),r&&this._events.next(new ga(this.pristine,t))}_updateTouched(e={},t){this.touched=this._anyControlsTouched(),this._events.next(new ya(this.touched,t)),this._parent&&!e.onlySelf&&this._parent._updateTouched(e,t)}_onDisabledChange=[];_registerOnCollectionChange(e){this._onCollectionChange=e}_setUpdateStrategy(e){Hc(e)&&e.updateOn!=null&&(this._updateOn=e.updateOn)}_parentMarkedDirty(e){let t=this._parent&&this._parent.dirty;return!e&&!!t&&!this._parent._anyControlsDirty()}_find(e){return null}_assignValidators(e){this._rawValidators=Array.isArray(e)?e.slice():e,this._composedValidatorFn=bA(this._rawValidators)}_assignAsyncValidators(e){this._rawAsyncValidators=Array.isArray(e)?e.slice():e,this._composedAsyncValidatorFn=SA(this._rawAsyncValidators)}};var _x=new We("",{providedIn:"root",factory:()=>mp}),mp="always";function CA(n,e){return[...e.path,n]}function TA(n,e,t=mp){AA(n,e),e.valueAccessor.writeValue(n.value),(n.disabled||t==="always")&&e.valueAccessor.setDisabledState?.(n.disabled),IA(n,e),NA(n,e),RA(n,e),DA(n,e)}function sx(n,e){n.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(e)})}function DA(n,e){if(e.valueAccessor.setDisabledState){let t=i=>{e.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(t),e._registerOnDestroy(()=>{n._unregisterOnDisabledChange(t)})}}function AA(n,e){let t=_A(n);e.validator!==null?n.setValidators(nx(t,e.validator)):typeof t=="function"&&n.setValidators([t]);let i=xA(n);e.asyncValidator!==null?n.setAsyncValidators(nx(i,e.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();sx(e._rawValidators,r),sx(e._rawAsyncValidators,r)}function IA(n,e){e.valueAccessor.registerOnChange(t=>{n._pendingValue=t,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&xx(n,e)})}function RA(n,e){e.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&xx(n,e),n.updateOn!=="submit"&&n.markAsTouched()})}function xx(n,e){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),e.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function NA(n,e){let t=(i,r)=>{e.valueAccessor.writeValue(i),r&&e.viewToModelUpdate(i)};n.registerOnChange(t),e._registerOnDestroy(()=>{n._unregisterOnChange(t)})}function PA(n,e){if(!n.hasOwnProperty("model"))return!1;let t=n.model;return t.isFirstChange()?!0:!Object.is(e,t.currentValue)}function OA(n){return Object.getPrototypeOf(n.constructor)===Vc}function LA(n,e){if(!e)return null;Array.isArray(e);let t,i,r;return e.forEach(s=>{s.constructor===Bc?t=s:OA(s)?i=s:r=s}),r||i||t||null}function ox(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function ax(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var FA=class extends pp{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(e=null,t,i){super(EA(t),wA(i,t)),this._applyFormState(e),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Hc(t)&&(t.nonNullable||t.initialValueIsDefault)&&(ax(e)?this.defaultValue=e.value:this.defaultValue=e)}setValue(e,t={}){this.value=this._pendingValue=e,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(e,t={}){this.setValue(e,t)}reset(e=this.defaultValue,t={}){this._applyFormState(e),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),this._pendingChange=!1}_updateValue(){}_anyControls(e){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(e){this._onChange.push(e)}_unregisterOnChange(e){ox(this._onChange,e)}registerOnDisabledChange(e){this._onDisabledChange.push(e)}_unregisterOnDisabledChange(e){ox(this._onDisabledChange,e)}_forEachChild(e){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(e){ax(e)?(this.value=this._pendingValue=e.value,e.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=e}};var kA={provide:va,useExisting:Zi(()=>gp)},lx=Promise.resolve(),gp=(()=>{class n extends va{_changeDetectorRef;callSetDisabledState;control=new FA;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new Tn;constructor(t,i,r,s,o,a){super(),this._changeDetectorRef=o,this.callSetDisabledState=a,this._parent=t,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=LA(this,s)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let i=t.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),PA(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective&&this.formDirective.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){TA(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){lx.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let i=t.isDisabled.currentValue,r=i!==0&&B0(i);lx.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?CA(t,this._parent):[t]}static \u0275fac=function(i){return new(i||n)(Je(fp,9),Je(pA,10),Je(mA,10),Je(_a,10),Je(qh,8),Je(_x,8))};static \u0275dir=cn({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Ps([kA]),Ti,ch]})}return n})();var UA={provide:_a,useExisting:Zi(()=>yp),multi:!0},yp=(()=>{class n extends Vc{writeValue(t){let i=t??"";this.setProperty("value",i)}registerOnChange(t){this.onChange=i=>{t(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ta(n)))(r||n)}})();static \u0275dir=cn({type:n,selectors:[["input","type","number","formControlName",""],["input","type","number","formControl",""],["input","type","number","ngModel",""]],hostBindings:function(i,r){i&1&&mn("input",function(o){return r.onChange(o.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[Ps([UA]),Ti]})}return n})();var VA={provide:_a,useExisting:Zi(()=>zc),multi:!0};function Mx(n,e){return n==null?`${e}`:(e&&typeof e=="object"&&(e="Object"),`${n}: ${e}`.slice(0,50))}function BA(n){return n.split(":")[0]}var zc=(()=>{class n extends Vc{value;_optionMap=new Map;_idCounter=0;set compareWith(t){this._compareWith=t}_compareWith=Object.is;writeValue(t){this.value=t;let i=this._getOptionId(t),r=Mx(i,t);this.setProperty("value",r)}registerOnChange(t){this.onChange=i=>{this.value=this._getOptionValue(i),t(this.value)}}_registerOption(){return(this._idCounter++).toString()}_getOptionId(t){for(let i of this._optionMap.keys())if(this._compareWith(this._optionMap.get(i),t))return i;return null}_getOptionValue(t){let i=BA(t);return this._optionMap.has(i)?this._optionMap.get(i):t}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ta(n)))(r||n)}})();static \u0275dir=cn({type:n,selectors:[["select","formControlName","",3,"multiple",""],["select","formControl","",3,"multiple",""],["select","ngModel","",3,"multiple",""]],hostBindings:function(i,r){i&1&&mn("change",function(o){return r.onChange(o.target.value)})("blur",function(){return r.onTouched()})},inputs:{compareWith:"compareWith"},standalone:!1,features:[Ps([VA]),Ti]})}return n})(),Ex=(()=>{class n{_element;_renderer;_select;id;constructor(t,i,r){this._element=t,this._renderer=i,this._select=r,this._select&&(this.id=this._select._registerOption())}set ngValue(t){this._select!=null&&(this._select._optionMap.set(this.id,t),this._setElementValue(Mx(this.id,t)),this._select.writeValue(this._select.value))}set value(t){this._setElementValue(t),this._select&&this._select.writeValue(this._select.value)}_setElementValue(t){this._renderer.setProperty(this._element.nativeElement,"value",t)}ngOnDestroy(){this._select&&(this._select._optionMap.delete(this.id),this._select.writeValue(this._select.value))}static \u0275fac=function(i){return new(i||n)(Je(ai),Je(Ds),Je(zc,9))};static \u0275dir=cn({type:n,selectors:[["option"]],inputs:{ngValue:"ngValue",value:"value"},standalone:!1})}return n})(),HA={provide:_a,useExisting:Zi(()=>bx),multi:!0};function cx(n,e){return n==null?`${e}`:(typeof e=="string"&&(e=`'${e}'`),e&&typeof e=="object"&&(e="Object"),`${n}: ${e}`.slice(0,50))}function zA(n){return n.split(":")[0]}var bx=(()=>{class n extends Vc{value;_optionMap=new Map;_idCounter=0;set compareWith(t){this._compareWith=t}_compareWith=Object.is;writeValue(t){this.value=t;let i;if(Array.isArray(t)){let r=t.map(s=>this._getOptionId(s));i=(s,o)=>{s._setSelected(r.indexOf(o.toString())>-1)}}else i=(r,s)=>{r._setSelected(!1)};this._optionMap.forEach(i)}registerOnChange(t){this.onChange=i=>{let r=[],s=i.selectedOptions;if(s!==void 0){let o=s;for(let a=0;a<o.length;a++){let l=o[a],c=this._getOptionValue(l.value);r.push(c)}}else{let o=i.options;for(let a=0;a<o.length;a++){let l=o[a];if(l.selected){let c=this._getOptionValue(l.value);r.push(c)}}}this.value=r,t(r)}}_registerOption(t){let i=(this._idCounter++).toString();return this._optionMap.set(i,t),i}_getOptionId(t){for(let i of this._optionMap.keys())if(this._compareWith(this._optionMap.get(i)._value,t))return i;return null}_getOptionValue(t){let i=zA(t);return this._optionMap.has(i)?this._optionMap.get(i)._value:t}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ta(n)))(r||n)}})();static \u0275dir=cn({type:n,selectors:[["select","multiple","","formControlName",""],["select","multiple","","formControl",""],["select","multiple","","ngModel",""]],hostBindings:function(i,r){i&1&&mn("change",function(o){return r.onChange(o.target)})("blur",function(){return r.onTouched()})},inputs:{compareWith:"compareWith"},standalone:!1,features:[Ps([HA]),Ti]})}return n})(),wx=(()=>{class n{_element;_renderer;_select;id;_value;constructor(t,i,r){this._element=t,this._renderer=i,this._select=r,this._select&&(this.id=this._select._registerOption(this))}set ngValue(t){this._select!=null&&(this._value=t,this._setElementValue(cx(this.id,t)),this._select.writeValue(this._select.value))}set value(t){this._select?(this._value=t,this._setElementValue(cx(this.id,t)),this._select.writeValue(this._select.value)):this._setElementValue(t)}_setElementValue(t){this._renderer.setProperty(this._element.nativeElement,"value",t)}_setSelected(t){this._renderer.setProperty(this._element.nativeElement,"selected",t)}ngOnDestroy(){this._select&&(this._select._optionMap.delete(this.id),this._select.writeValue(this._select.value))}static \u0275fac=function(i){return new(i||n)(Je(ai),Je(Ds),Je(bx,9))};static \u0275dir=cn({type:n,selectors:[["option"]],inputs:{ngValue:"ngValue",value:"value"},standalone:!1})}return n})();var GA=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=Is({type:n});static \u0275inj=bs({})}return n})();var Sx=(()=>{class n{static withConfig(t){return{ngModule:n,providers:[{provide:_x,useValue:t.callSetDisabledState??mp}]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=Is({type:n});static \u0275inj=bs({imports:[GA]})}return n})();var es={LEFT:0,MIDDLE:1,RIGHT:2,ROTATE:0,DOLLY:1,PAN:2},ts={ROTATE:0,PAN:1,DOLLY_PAN:2,DOLLY_ROTATE:3},jA=0,Cx=1,$A=2;var VM=1,qA=2,Li=3,cr=0,Yt=1,un=2,hi=0,oo=1,Tx=2,Dx=3,Ax=4,XA=5,Br=100,YA=101,ZA=102,KA=103,JA=104,QA=200,eI=201,tI=202,nI=203,Kp=204,Jp=205,iI=206,rI=207,sI=208,oI=209,aI=210,lI=211,cI=212,uI=213,dI=214,Qp=0,em=1,tm=2,uo=3,nm=4,im=5,rm=6,sm=7,xg=0,fI=1,hI=2,pi=0,Mg=1,Eg=2,bg=3,wg=4,pI=5,Sg=6,Cg=7;var Ix=300,fo=301,ho=302,om=303,am=304,ed=306,lm=1e3,zr=1001,cm=1002,_n=1003,mI=1004;var Gc=1005;var vn=1006,vp=1007;var Gr=1008;var Ui=1009,BM=1010,HM=1011,ka=1012,Tg=1013,Wr=1014,di=1015,ns=1016,Dg=1017,Ag=1018,po=1020,zM=35902,GM=1021,WM=1022,$n=1023,jM=1024,$M=1025,ao=1026,mo=1027,Ig=1028,Rg=1029,qM=1030,Ng=1031;var Pg=1033,vu=33776,_u=33777,xu=33778,Mu=33779,um=35840,dm=35841,fm=35842,hm=35843,pm=36196,mm=37492,gm=37496,ym=37808,vm=37809,_m=37810,xm=37811,Mm=37812,Em=37813,bm=37814,wm=37815,Sm=37816,Cm=37817,Tm=37818,Dm=37819,Am=37820,Im=37821,Eu=36492,Rm=36494,Nm=36495,XM=36283,Pm=36284,Om=36285,Lm=36286;var bu=2300,Fm=2301,_p=2302,Rx=2400,Nx=2401,Px=2402;var gI=3200,yI=3201;var Og=0,vI=1,ar="",rn="srgb",So="srgb-linear",td="linear",ot="srgb";var Vs=7680;var Ox=519,_I=512,xI=513,MI=514,YM=515,EI=516,bI=517,wI=518,SI=519,km=35044,ZM=35048;var Lx="300 es",Fi=2e3,wu=2001,Vi=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){if(this._listeners===void 0)return!1;let i=this._listeners;return i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){if(this._listeners===void 0)return;let r=this._listeners[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){if(this._listeners===void 0)return;let i=this._listeners[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},qt=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"],Fx=1234567,Ra=Math.PI/180,Ua=180/Math.PI;function mi(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(qt[n&255]+qt[n>>8&255]+qt[n>>16&255]+qt[n>>24&255]+"-"+qt[e&255]+qt[e>>8&255]+"-"+qt[e>>16&15|64]+qt[e>>24&255]+"-"+qt[t&63|128]+qt[t>>8&255]+"-"+qt[t>>16&255]+qt[t>>24&255]+qt[i&255]+qt[i>>8&255]+qt[i>>16&255]+qt[i>>24&255]).toLowerCase()}function Bt(n,e,t){return Math.max(e,Math.min(t,n))}function Lg(n,e){return(n%e+e)%e}function CI(n,e,t,i,r){return i+(n-e)*(r-i)/(t-e)}function TI(n,e,t){return n!==e?(t-n)/(e-n):0}function Na(n,e,t){return(1-t)*n+t*e}function DI(n,e,t,i){return Na(n,e,1-Math.exp(-t*i))}function AI(n,e=1){return e-Math.abs(Lg(n,e*2)-e)}function II(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*(3-2*n))}function RI(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*n*(n*(n*6-15)+10))}function NI(n,e){return n+Math.floor(Math.random()*(e-n+1))}function PI(n,e){return n+Math.random()*(e-n)}function OI(n){return n*(.5-Math.random())}function LI(n){n!==void 0&&(Fx=n);let e=Fx+=1831565813;return e=Math.imul(e^e>>>15,e|1),e^=e+Math.imul(e^e>>>7,e|61),((e^e>>>14)>>>0)/4294967296}function FI(n){return n*Ra}function kI(n){return n*Ua}function UI(n){return(n&n-1)===0&&n!==0}function VI(n){return Math.pow(2,Math.ceil(Math.log(n)/Math.LN2))}function BI(n){return Math.pow(2,Math.floor(Math.log(n)/Math.LN2))}function HI(n,e,t,i,r){let s=Math.cos,o=Math.sin,a=s(t/2),l=o(t/2),c=s((e+i)/2),u=o((e+i)/2),d=s((e-i)/2),h=o((e-i)/2),f=s((i-e)/2),m=o((i-e)/2);switch(r){case"XYX":n.set(a*u,l*d,l*h,a*c);break;case"YZY":n.set(l*h,a*u,l*d,a*c);break;case"ZXZ":n.set(l*d,l*h,a*u,a*c);break;case"XZX":n.set(a*u,l*m,l*f,a*c);break;case"YXY":n.set(l*f,a*u,l*m,a*c);break;case"ZYZ":n.set(l*m,l*f,a*u,a*c);break;default:console.warn("THREE.MathUtils: .setQuaternionFromProperEuler() encountered an unknown order: "+r)}}function jn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function ut(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var KM={DEG2RAD:Ra,RAD2DEG:Ua,generateUUID:mi,clamp:Bt,euclideanModulo:Lg,mapLinear:CI,inverseLerp:TI,lerp:Na,damp:DI,pingpong:AI,smoothstep:II,smootherstep:RI,randInt:NI,randFloat:PI,randFloatSpread:OI,seededRandom:LI,degToRad:FI,radToDeg:kI,isPowerOfTwo:UI,ceilPowerOfTwo:VI,floorPowerOfTwo:BI,setQuaternionFromProperEuler:HI,normalize:ut,denormalize:jn},te=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Bt(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Fe=class n{constructor(e,t,i,r,s,o,a,l,c){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c)}set(e,t,i,r,s,o,a,l,c){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=l,u[6]=i,u[7]=o,u[8]=c,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],l=i[6],c=i[1],u=i[4],d=i[7],h=i[2],f=i[5],m=i[8],v=r[0],g=r[3],p=r[6],M=r[1],_=r[4],y=r[7],A=r[2],C=r[5],S=r[8];return s[0]=o*v+a*M+l*A,s[3]=o*g+a*_+l*C,s[6]=o*p+a*y+l*S,s[1]=c*v+u*M+d*A,s[4]=c*g+u*_+d*C,s[7]=c*p+u*y+d*S,s[2]=h*v+f*M+m*A,s[5]=h*g+f*_+m*C,s[8]=h*p+f*y+m*S,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8];return t*o*u-t*a*c-i*s*u+i*a*l+r*s*c-r*o*l}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=u*o-a*c,h=a*l-u*s,f=c*s-o*l,m=t*d+i*h+r*f;if(m===0)return this.set(0,0,0,0,0,0,0,0,0);let v=1/m;return e[0]=d*v,e[1]=(r*c-u*i)*v,e[2]=(a*i-r*o)*v,e[3]=h*v,e[4]=(u*t-r*l)*v,e[5]=(r*s-a*t)*v,e[6]=f*v,e[7]=(i*l-c*t)*v,e[8]=(o*t-i*s)*v,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let l=Math.cos(s),c=Math.sin(s);return this.set(i*l,i*c,-i*(l*o+c*a)+o+e,-r*c,r*l,-r*(-c*o+l*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(xp.makeScale(e,t)),this}rotate(e){return this.premultiply(xp.makeRotation(-e)),this}translate(e,t){return this.premultiply(xp.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},xp=new Fe;function JM(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function Su(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function zI(){let n=Su("canvas");return n.style.display="block",n}var kx={};function Aa(n){n in kx||(kx[n]=!0,console.warn(n))}function GI(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}function WI(n){let e=n.elements;e[2]=.5*e[2]+.5*e[3],e[6]=.5*e[6]+.5*e[7],e[10]=.5*e[10]+.5*e[11],e[14]=.5*e[14]+.5*e[15]}function jI(n){let e=n.elements;e[11]===-1?(e[10]=-e[10]-1,e[14]=-e[14]):(e[10]=-e[10],e[14]=-e[14]+1)}var Xe={enabled:!0,workingColorSpace:So,spaces:{},convert:function(n,e,t){return this.enabled===!1||e===t||!e||!t||(this.spaces[e].transfer===ot&&(n.r=ki(n.r),n.g=ki(n.g),n.b=ki(n.b)),this.spaces[e].primaries!==this.spaces[t].primaries&&(n.applyMatrix3(this.spaces[e].toXYZ),n.applyMatrix3(this.spaces[t].fromXYZ)),this.spaces[t].transfer===ot&&(n.r=lo(n.r),n.g=lo(n.g),n.b=lo(n.b))),n},fromWorkingColorSpace:function(n,e){return this.convert(n,this.workingColorSpace,e)},toWorkingColorSpace:function(n,e){return this.convert(n,e,this.workingColorSpace)},getPrimaries:function(n){return this.spaces[n].primaries},getTransfer:function(n){return n===ar?td:this.spaces[n].transfer},getLuminanceCoefficients:function(n,e=this.workingColorSpace){return n.fromArray(this.spaces[e].luminanceCoefficients)},define:function(n){Object.assign(this.spaces,n)},_getMatrix:function(n,e,t){return n.copy(this.spaces[e].toXYZ).multiply(this.spaces[t].fromXYZ)},_getDrawingBufferColorSpace:function(n){return this.spaces[n].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(n=this.workingColorSpace){return this.spaces[n].workingColorSpaceConfig.unpackColorSpace}};function ki(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function lo(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var Ux=[.64,.33,.3,.6,.15,.06],Vx=[.2126,.7152,.0722],Bx=[.3127,.329],Hx=new Fe().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),zx=new Fe().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);Xe.define({[So]:{primaries:Ux,whitePoint:Bx,transfer:td,toXYZ:Hx,fromXYZ:zx,luminanceCoefficients:Vx,workingColorSpaceConfig:{unpackColorSpace:rn},outputColorSpaceConfig:{drawingBufferColorSpace:rn}},[rn]:{primaries:Ux,whitePoint:Bx,transfer:ot,toXYZ:Hx,fromXYZ:zx,luminanceCoefficients:Vx,outputColorSpaceConfig:{drawingBufferColorSpace:rn}}});var Bs,Um=class{static getDataURL(e){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let t;if(e instanceof HTMLCanvasElement)t=e;else{Bs===void 0&&(Bs=Su("canvas")),Bs.width=e.width,Bs.height=e.height;let i=Bs.getContext("2d");e instanceof ImageData?i.putImageData(e,0,0):i.drawImage(e,0,0,e.width,e.height),t=Bs}return t.width>2048||t.height>2048?(console.warn("THREE.ImageUtils.getDataURL: Image converted to jpg for performance reasons",e),t.toDataURL("image/jpeg",.6)):t.toDataURL("image/png")}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Su("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=ki(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(ki(t[i]/255)*255):t[i]=ki(t[i]);return{data:t,width:e.width,height:e.height}}else return console.warn("THREE.ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},$I=0,Cu=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:$I++}),this.uuid=mi(),this.data=e,this.dataReady=!0,this.version=0}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Mp(r[o].image)):s.push(Mp(r[o]))}else s=Mp(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Mp(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Um.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(console.warn("THREE.Texture: Unable to serialize Texture."),{})}var qI=0,zi=(()=>{class n extends Vi{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=zr,s=zr,o=vn,a=Gr,l=$n,c=Ui,u=n.DEFAULT_ANISOTROPY,d=ar){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:qI++}),this.uuid=mi(),this.name="",this.source=new Cu(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=l,this.internalFormat=null,this.type=c,this.offset=new te(0,0),this.repeat=new te(1,1),this.center=new te(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Fe,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.version=0,this.onUpdate=null,this.isRenderTargetTexture=!1,this.pmremVersion=0}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.6,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==Ix)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case lm:t.x=t.x-Math.floor(t.x);break;case zr:t.x=t.x<0?0:1;break;case cm:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case lm:t.y=t.y-Math.floor(t.y);break;case zr:t.y=t.y<0?0:1;break;case cm:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=Ix,n.DEFAULT_ANISOTROPY=1,n})(),At=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,l=e.elements,c=l[0],u=l[4],d=l[8],h=l[1],f=l[5],m=l[9],v=l[2],g=l[6],p=l[10];if(Math.abs(u-h)<.01&&Math.abs(d-v)<.01&&Math.abs(m-g)<.01){if(Math.abs(u+h)<.1&&Math.abs(d+v)<.1&&Math.abs(m+g)<.1&&Math.abs(c+f+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let _=(c+1)/2,y=(f+1)/2,A=(p+1)/2,C=(u+h)/4,S=(d+v)/4,I=(m+g)/4;return _>y&&_>A?_<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(_),r=C/i,s=S/i):y>A?y<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(y),i=C/r,s=I/r):A<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(A),i=S/s,r=I/s),this.set(i,r,s,t),this}let M=Math.sqrt((g-m)*(g-m)+(d-v)*(d-v)+(h-u)*(h-u));return Math.abs(M)<.001&&(M=1),this.x=(g-m)/M,this.y=(d-v)/M,this.z=(h-u)/M,this.w=Math.acos((c+f+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this.w=Math.max(e.w,Math.min(t.w,this.w)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this.w=Math.max(e,Math.min(t,this.w)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},Vm=class extends Vi{constructor(e=1,t=1,i={}){super(),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=1,this.scissor=new At(0,0,e,t),this.scissorTest=!1,this.viewport=new At(0,0,e,t);let r={width:e,height:t,depth:1};i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:vn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1},i);let s=new zi(r,i.mapping,i.wrapS,i.wrapT,i.magFilter,i.minFilter,i.format,i.type,i.anisotropy,i.colorSpace);s.flipY=!1,s.generateMipmaps=i.generateMipmaps,s.internalFormat=i.internalFormat,this.textures=[];let o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0;this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this.depthTexture=i.depthTexture,this.samples=i.samples}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i;this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let i=0,r=e.textures.length;i<r;i++)this.textures[i]=e.textures[i].clone(),this.textures[i].isRenderTargetTexture=!0;let t=Object.assign({},e.texture.image);return this.texture.source=new Cu(t),this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},xn=class extends Vm{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Tu=class extends zi{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=_n,this.minFilter=_n,this.wrapR=zr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Bm=class extends zi{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=_n,this.minFilter=_n,this.wrapR=zr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Pt=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let l=i[r+0],c=i[r+1],u=i[r+2],d=i[r+3],h=s[o+0],f=s[o+1],m=s[o+2],v=s[o+3];if(a===0){e[t+0]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d;return}if(a===1){e[t+0]=h,e[t+1]=f,e[t+2]=m,e[t+3]=v;return}if(d!==v||l!==h||c!==f||u!==m){let g=1-a,p=l*h+c*f+u*m+d*v,M=p>=0?1:-1,_=1-p*p;if(_>Number.EPSILON){let A=Math.sqrt(_),C=Math.atan2(A,p*M);g=Math.sin(g*C)/A,a=Math.sin(a*C)/A}let y=a*M;if(l=l*g+h*y,c=c*g+f*y,u=u*g+m*y,d=d*g+v*y,g===1-a){let A=1/Math.sqrt(l*l+c*c+u*u+d*d);l*=A,c*=A,u*=A,d*=A}}e[t]=l,e[t+1]=c,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],l=i[r+1],c=i[r+2],u=i[r+3],d=s[o],h=s[o+1],f=s[o+2],m=s[o+3];return e[t]=a*m+u*d+l*f-c*h,e[t+1]=l*m+u*h+c*d-a*f,e[t+2]=c*m+u*f+a*h-l*d,e[t+3]=u*m-a*d-l*h-c*f,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,l=Math.sin,c=a(i/2),u=a(r/2),d=a(s/2),h=l(i/2),f=l(r/2),m=l(s/2);switch(o){case"XYZ":this._x=h*u*d+c*f*m,this._y=c*f*d-h*u*m,this._z=c*u*m+h*f*d,this._w=c*u*d-h*f*m;break;case"YXZ":this._x=h*u*d+c*f*m,this._y=c*f*d-h*u*m,this._z=c*u*m-h*f*d,this._w=c*u*d+h*f*m;break;case"ZXY":this._x=h*u*d-c*f*m,this._y=c*f*d+h*u*m,this._z=c*u*m+h*f*d,this._w=c*u*d-h*f*m;break;case"ZYX":this._x=h*u*d-c*f*m,this._y=c*f*d+h*u*m,this._z=c*u*m-h*f*d,this._w=c*u*d+h*f*m;break;case"YZX":this._x=h*u*d+c*f*m,this._y=c*f*d+h*u*m,this._z=c*u*m-h*f*d,this._w=c*u*d-h*f*m;break;case"XZY":this._x=h*u*d-c*f*m,this._y=c*f*d-h*u*m,this._z=c*u*m+h*f*d,this._w=c*u*d+h*f*m;break;default:console.warn("THREE.Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],l=t[9],c=t[2],u=t[6],d=t[10],h=i+a+d;if(h>0){let f=.5/Math.sqrt(h+1);this._w=.25/f,this._x=(u-l)*f,this._y=(s-c)*f,this._z=(o-r)*f}else if(i>a&&i>d){let f=2*Math.sqrt(1+i-a-d);this._w=(u-l)/f,this._x=.25*f,this._y=(r+o)/f,this._z=(s+c)/f}else if(a>d){let f=2*Math.sqrt(1+a-i-d);this._w=(s-c)/f,this._x=(r+o)/f,this._y=.25*f,this._z=(l+u)/f}else{let f=2*Math.sqrt(1+d-i-a);this._w=(o-r)/f,this._x=(s+c)/f,this._y=(l+u)/f,this._z=.25*f}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<Number.EPSILON?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(Bt(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,l=t._y,c=t._z,u=t._w;return this._x=i*u+o*a+r*c-s*l,this._y=r*u+o*l+s*a-i*c,this._z=s*u+o*c+i*l-r*a,this._w=o*u-i*a-r*l-s*c,this._onChangeCallback(),this}slerp(e,t){if(t===0)return this;if(t===1)return this.copy(e);let i=this._x,r=this._y,s=this._z,o=this._w,a=o*e._w+i*e._x+r*e._y+s*e._z;if(a<0?(this._w=-e._w,this._x=-e._x,this._y=-e._y,this._z=-e._z,a=-a):this.copy(e),a>=1)return this._w=o,this._x=i,this._y=r,this._z=s,this;let l=1-a*a;if(l<=Number.EPSILON){let f=1-t;return this._w=f*o+t*this._w,this._x=f*i+t*this._x,this._y=f*r+t*this._y,this._z=f*s+t*this._z,this.normalize(),this}let c=Math.sqrt(l),u=Math.atan2(c,a),d=Math.sin((1-t)*u)/c,h=Math.sin(t*u)/c;return this._w=o*d+this._w*h,this._x=i*d+this._x*h,this._y=r*d+this._y*h,this._z=s*d+this._z*h,this._onChangeCallback(),this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},R=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(Gx.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(Gx.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,l=e.w,c=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+l*c+o*d-a*u,this.y=i+l*u+a*c-s*d,this.z=r+l*d+s*u-o*c,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=Math.max(e.x,Math.min(t.x,this.x)),this.y=Math.max(e.y,Math.min(t.y,this.y)),this.z=Math.max(e.z,Math.min(t.z,this.z)),this}clampScalar(e,t){return this.x=Math.max(e,Math.min(t,this.x)),this.y=Math.max(e,Math.min(t,this.y)),this.z=Math.max(e,Math.min(t,this.z)),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(Math.max(e,Math.min(t,i)))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,l=t.z;return this.x=r*l-s*a,this.y=s*o-i*l,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return Ep.copy(this).projectOnVector(e),this.sub(Ep)}reflect(e){return this.sub(Ep.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(Bt(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},Ep=new R,Gx=new Pt,Bi=class{constructor(e=new R(1/0,1/0,1/0),t=new R(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(Hn.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(Hn.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=Hn.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,Hn):Hn.fromBufferAttribute(s,o),Hn.applyMatrix4(e.matrixWorld),this.expandByPoint(Hn);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),Wc.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),Wc.copy(i.boundingBox)),Wc.applyMatrix4(e.matrixWorld),this.union(Wc)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,Hn),Hn.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(xa),jc.subVectors(this.max,xa),Hs.subVectors(e.a,xa),zs.subVectors(e.b,xa),Gs.subVectors(e.c,xa),tr.subVectors(zs,Hs),nr.subVectors(Gs,zs),Pr.subVectors(Hs,Gs);let t=[0,-tr.z,tr.y,0,-nr.z,nr.y,0,-Pr.z,Pr.y,tr.z,0,-tr.x,nr.z,0,-nr.x,Pr.z,0,-Pr.x,-tr.y,tr.x,0,-nr.y,nr.x,0,-Pr.y,Pr.x,0];return!bp(t,Hs,zs,Gs,jc)||(t=[1,0,0,0,1,0,0,0,1],!bp(t,Hs,zs,Gs,jc))?!1:($c.crossVectors(tr,nr),t=[$c.x,$c.y,$c.z],bp(t,Hs,zs,Gs,jc))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,Hn).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(Hn).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Ii[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Ii[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Ii[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Ii[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Ii[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Ii[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Ii[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Ii[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Ii),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}},Ii=[new R,new R,new R,new R,new R,new R,new R,new R],Hn=new R,Wc=new Bi,Hs=new R,zs=new R,Gs=new R,tr=new R,nr=new R,Pr=new R,xa=new R,jc=new R,$c=new R,Or=new R;function bp(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){Or.fromArray(n,s);let a=r.x*Math.abs(Or.x)+r.y*Math.abs(Or.y)+r.z*Math.abs(Or.z),l=e.dot(Or),c=t.dot(Or),u=i.dot(Or);if(Math.max(-Math.max(l,c,u),Math.min(l,c,u))>a)return!1}return!0}var XI=new Bi,Ma=new R,wp=new R,jr=class{constructor(e=new R,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):XI.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;Ma.subVectors(e,this.center);let t=Ma.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(Ma,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(wp.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(Ma.copy(e.center).add(wp)),this.expandByPoint(Ma.copy(e.center).sub(wp))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}},Ri=new R,Sp=new R,qc=new R,ir=new R,Cp=new R,Xc=new R,Tp=new R,Va=class{constructor(e=new R,t=new R(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Ri)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Ri.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Ri.copy(this.origin).addScaledVector(this.direction,t),Ri.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Sp.copy(e).add(t).multiplyScalar(.5),qc.copy(t).sub(e).normalize(),ir.copy(this.origin).sub(Sp);let s=e.distanceTo(t)*.5,o=-this.direction.dot(qc),a=ir.dot(this.direction),l=-ir.dot(qc),c=ir.lengthSq(),u=Math.abs(1-o*o),d,h,f,m;if(u>0)if(d=o*l-a,h=o*a-l,m=s*u,d>=0)if(h>=-m)if(h<=m){let v=1/u;d*=v,h*=v,f=d*(d+o*h+2*a)+h*(o*d+h+2*l)+c}else h=s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;else h=-s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;else h<=-m?(d=Math.max(0,-(-o*s+a)),h=d>0?-s:Math.min(Math.max(-s,-l),s),f=-d*d+h*(h+2*l)+c):h<=m?(d=0,h=Math.min(Math.max(-s,-l),s),f=h*(h+2*l)+c):(d=Math.max(0,-(o*s+a)),h=d>0?s:Math.min(Math.max(-s,-l),s),f=-d*d+h*(h+2*l)+c);else h=o>0?-s:s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*l)+c;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(Sp).addScaledVector(qc,h),f}intersectSphere(e,t){Ri.subVectors(e.center,this.origin);let i=Ri.dot(this.direction),r=Ri.dot(Ri)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,l=i+o;return l<0?null:a<0?this.at(l,t):this.at(a,t)}intersectsSphere(e){return this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,l,c=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,h=this.origin;return c>=0?(i=(e.min.x-h.x)*c,r=(e.max.x-h.x)*c):(i=(e.max.x-h.x)*c,r=(e.min.x-h.x)*c),u>=0?(s=(e.min.y-h.y)*u,o=(e.max.y-h.y)*u):(s=(e.max.y-h.y)*u,o=(e.min.y-h.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-h.z)*d,l=(e.max.z-h.z)*d):(a=(e.max.z-h.z)*d,l=(e.min.z-h.z)*d),i>l||a>r)||((a>i||i!==i)&&(i=a),(l<r||r!==r)&&(r=l),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Ri)!==null}intersectTriangle(e,t,i,r,s){Cp.subVectors(t,e),Xc.subVectors(i,e),Tp.crossVectors(Cp,Xc);let o=this.direction.dot(Tp),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;ir.subVectors(this.origin,e);let l=a*this.direction.dot(Xc.crossVectors(ir,Xc));if(l<0)return null;let c=a*this.direction.dot(Cp.cross(ir));if(c<0||l+c>o)return null;let u=-a*ir.dot(Tp);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},tt=class n{constructor(e,t,i,r,s,o,a,l,c,u,d,h,f,m,v,g){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,l,c,u,d,h,f,m,v,g)}set(e,t,i,r,s,o,a,l,c,u,d,h,f,m,v,g){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=l,p[2]=c,p[6]=u,p[10]=d,p[14]=h,p[3]=f,p[7]=m,p[11]=v,p[15]=g,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){let t=this.elements,i=e.elements,r=1/Ws.setFromMatrixColumn(e,0).length(),s=1/Ws.setFromMatrixColumn(e,1).length(),o=1/Ws.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),l=Math.cos(r),c=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let h=o*u,f=o*d,m=a*u,v=a*d;t[0]=l*u,t[4]=-l*d,t[8]=c,t[1]=f+m*c,t[5]=h-v*c,t[9]=-a*l,t[2]=v-h*c,t[6]=m+f*c,t[10]=o*l}else if(e.order==="YXZ"){let h=l*u,f=l*d,m=c*u,v=c*d;t[0]=h+v*a,t[4]=m*a-f,t[8]=o*c,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=f*a-m,t[6]=v+h*a,t[10]=o*l}else if(e.order==="ZXY"){let h=l*u,f=l*d,m=c*u,v=c*d;t[0]=h-v*a,t[4]=-o*d,t[8]=m+f*a,t[1]=f+m*a,t[5]=o*u,t[9]=v-h*a,t[2]=-o*c,t[6]=a,t[10]=o*l}else if(e.order==="ZYX"){let h=o*u,f=o*d,m=a*u,v=a*d;t[0]=l*u,t[4]=m*c-f,t[8]=h*c+v,t[1]=l*d,t[5]=v*c+h,t[9]=f*c-m,t[2]=-c,t[6]=a*l,t[10]=o*l}else if(e.order==="YZX"){let h=o*l,f=o*c,m=a*l,v=a*c;t[0]=l*u,t[4]=v-h*d,t[8]=m*d+f,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-c*u,t[6]=f*d+m,t[10]=h-v*d}else if(e.order==="XZY"){let h=o*l,f=o*c,m=a*l,v=a*c;t[0]=l*u,t[4]=-d,t[8]=c*u,t[1]=h*d+v,t[5]=o*u,t[9]=f*d-m,t[2]=m*d-f,t[6]=a*u,t[10]=v*d+h}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(YI,e,ZI)}lookAt(e,t,i){let r=this.elements;return gn.subVectors(e,t),gn.lengthSq()===0&&(gn.z=1),gn.normalize(),rr.crossVectors(i,gn),rr.lengthSq()===0&&(Math.abs(i.z)===1?gn.x+=1e-4:gn.z+=1e-4,gn.normalize(),rr.crossVectors(i,gn)),rr.normalize(),Yc.crossVectors(gn,rr),r[0]=rr.x,r[4]=Yc.x,r[8]=gn.x,r[1]=rr.y,r[5]=Yc.y,r[9]=gn.y,r[2]=rr.z,r[6]=Yc.z,r[10]=gn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],l=i[8],c=i[12],u=i[1],d=i[5],h=i[9],f=i[13],m=i[2],v=i[6],g=i[10],p=i[14],M=i[3],_=i[7],y=i[11],A=i[15],C=r[0],S=r[4],I=r[8],b=r[12],x=r[1],D=r[5],N=r[9],O=r[13],U=r[2],$=r[6],W=r[10],q=r[14],z=r[3],Q=r[7],ee=r[11],oe=r[15];return s[0]=o*C+a*x+l*U+c*z,s[4]=o*S+a*D+l*$+c*Q,s[8]=o*I+a*N+l*W+c*ee,s[12]=o*b+a*O+l*q+c*oe,s[1]=u*C+d*x+h*U+f*z,s[5]=u*S+d*D+h*$+f*Q,s[9]=u*I+d*N+h*W+f*ee,s[13]=u*b+d*O+h*q+f*oe,s[2]=m*C+v*x+g*U+p*z,s[6]=m*S+v*D+g*$+p*Q,s[10]=m*I+v*N+g*W+p*ee,s[14]=m*b+v*O+g*q+p*oe,s[3]=M*C+_*x+y*U+A*z,s[7]=M*S+_*D+y*$+A*Q,s[11]=M*I+_*N+y*W+A*ee,s[15]=M*b+_*O+y*q+A*oe,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],l=e[9],c=e[13],u=e[2],d=e[6],h=e[10],f=e[14],m=e[3],v=e[7],g=e[11],p=e[15];return m*(+s*l*d-r*c*d-s*a*h+i*c*h+r*a*f-i*l*f)+v*(+t*l*f-t*c*h+s*o*h-r*o*f+r*c*u-s*l*u)+g*(+t*c*d-t*a*f-s*o*d+i*o*f+s*a*u-i*c*u)+p*(-r*a*u-t*l*d+t*a*h+r*o*d-i*o*h+i*l*u)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],l=e[6],c=e[7],u=e[8],d=e[9],h=e[10],f=e[11],m=e[12],v=e[13],g=e[14],p=e[15],M=d*g*c-v*h*c+v*l*f-a*g*f-d*l*p+a*h*p,_=m*h*c-u*g*c-m*l*f+o*g*f+u*l*p-o*h*p,y=u*v*c-m*d*c+m*a*f-o*v*f-u*a*p+o*d*p,A=m*d*l-u*v*l-m*a*h+o*v*h+u*a*g-o*d*g,C=t*M+i*_+r*y+s*A;if(C===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let S=1/C;return e[0]=M*S,e[1]=(v*h*s-d*g*s-v*r*f+i*g*f+d*r*p-i*h*p)*S,e[2]=(a*g*s-v*l*s+v*r*c-i*g*c-a*r*p+i*l*p)*S,e[3]=(d*l*s-a*h*s-d*r*c+i*h*c+a*r*f-i*l*f)*S,e[4]=_*S,e[5]=(u*g*s-m*h*s+m*r*f-t*g*f-u*r*p+t*h*p)*S,e[6]=(m*l*s-o*g*s-m*r*c+t*g*c+o*r*p-t*l*p)*S,e[7]=(o*h*s-u*l*s+u*r*c-t*h*c-o*r*f+t*l*f)*S,e[8]=y*S,e[9]=(m*d*s-u*v*s-m*i*f+t*v*f+u*i*p-t*d*p)*S,e[10]=(o*v*s-m*a*s+m*i*c-t*v*c-o*i*p+t*a*p)*S,e[11]=(u*a*s-o*d*s-u*i*c+t*d*c+o*i*f-t*a*f)*S,e[12]=A*S,e[13]=(u*v*r-m*d*r+m*i*h-t*v*h-u*i*g+t*d*g)*S,e[14]=(m*a*r-o*v*r-m*i*l+t*v*l+o*i*g-t*a*g)*S,e[15]=(o*d*r-u*a*r+u*i*l-t*d*l-o*i*h+t*a*h)*S,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,l=e.z,c=s*o,u=s*a;return this.set(c*o+i,c*a-r*l,c*l+r*a,0,c*a+r*l,u*a+i,u*l-r*o,0,c*l-r*a,u*l+r*o,s*l*l+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,l=t._w,c=s+s,u=o+o,d=a+a,h=s*c,f=s*u,m=s*d,v=o*u,g=o*d,p=a*d,M=l*c,_=l*u,y=l*d,A=i.x,C=i.y,S=i.z;return r[0]=(1-(v+p))*A,r[1]=(f+y)*A,r[2]=(m-_)*A,r[3]=0,r[4]=(f-y)*C,r[5]=(1-(h+p))*C,r[6]=(g+M)*C,r[7]=0,r[8]=(m+_)*S,r[9]=(g-M)*S,r[10]=(1-(h+v))*S,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements,s=Ws.set(r[0],r[1],r[2]).length(),o=Ws.set(r[4],r[5],r[6]).length(),a=Ws.set(r[8],r[9],r[10]).length();this.determinant()<0&&(s=-s),e.x=r[12],e.y=r[13],e.z=r[14],zn.copy(this);let c=1/s,u=1/o,d=1/a;return zn.elements[0]*=c,zn.elements[1]*=c,zn.elements[2]*=c,zn.elements[4]*=u,zn.elements[5]*=u,zn.elements[6]*=u,zn.elements[8]*=d,zn.elements[9]*=d,zn.elements[10]*=d,t.setFromRotationMatrix(zn),i.x=s,i.y=o,i.z=a,this}makePerspective(e,t,i,r,s,o,a=Fi){let l=this.elements,c=2*s/(t-e),u=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),f,m;if(a===Fi)f=-(o+s)/(o-s),m=-2*o*s/(o-s);else if(a===wu)f=-o/(o-s),m=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=c,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=u,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=f,l[14]=m,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=Fi){let l=this.elements,c=1/(t-e),u=1/(i-r),d=1/(o-s),h=(t+e)*c,f=(i+r)*u,m,v;if(a===Fi)m=(o+s)*d,v=-2*d;else if(a===wu)m=s*d,v=-1*d;else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=2*c,l[4]=0,l[8]=0,l[12]=-h,l[1]=0,l[5]=2*u,l[9]=0,l[13]=-f,l[2]=0,l[6]=0,l[10]=v,l[14]=-m,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},Ws=new R,zn=new tt,YI=new R(0,0,0),ZI=new R(1,1,1),rr=new R,Yc=new R,gn=new R,Wx=new tt,jx=new Pt,$r=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],l=s[8],c=s[1],u=s[5],d=s[9],h=s[2],f=s[6],m=s[10];switch(i){case"XYZ":this._y=Math.asin(Bt(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,m),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(f,u),this._z=0);break;case"YXZ":this._x=Math.asin(-Bt(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(l,m),this._z=Math.atan2(c,u)):(this._y=Math.atan2(-h,o),this._z=0);break;case"ZXY":this._x=Math.asin(Bt(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(-h,m),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(c,o));break;case"ZYX":this._y=Math.asin(-Bt(h,-1,1)),Math.abs(h)<.9999999?(this._x=Math.atan2(f,m),this._z=Math.atan2(c,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(Bt(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-h,o)):(this._x=0,this._y=Math.atan2(l,m));break;case"XZY":this._z=Math.asin(-Bt(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(f,u),this._y=Math.atan2(l,o)):(this._x=Math.atan2(-d,m),this._y=0);break;default:console.warn("THREE.Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return Wx.makeRotationFromQuaternion(t),this.setFromRotationMatrix(Wx,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return jx.setFromEuler(this),this.setFromQuaternion(jx,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Du=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},KI=0,$x=new R,js=new Pt,Ni=new tt,Zc=new R,Ea=new R,JI=new R,QI=new Pt,qx=new R(1,0,0),Xx=new R(0,1,0),Yx=new R(0,0,1),Zx={type:"added"},e1={type:"removed"},$s={type:"childadded",child:null},Dp={type:"childremoved",child:null},qn=(()=>{class n extends Vi{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:KI++}),this.uuid=mi(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new R,i=new $r,r=new Pt,s=new R(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new tt},normalMatrix:{value:new Fe}}),this.matrix=new tt,this.matrixWorld=new tt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Du,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.userData={}}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return js.setFromAxisAngle(t,i),this.quaternion.multiply(js),this}rotateOnWorldAxis(t,i){return js.setFromAxisAngle(t,i),this.quaternion.premultiply(js),this}rotateX(t){return this.rotateOnAxis(qx,t)}rotateY(t){return this.rotateOnAxis(Xx,t)}rotateZ(t){return this.rotateOnAxis(Yx,t)}translateOnAxis(t,i){return $x.copy(t).applyQuaternion(this.quaternion),this.position.add($x.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(qx,t)}translateY(t){return this.translateOnAxis(Xx,t)}translateZ(t){return this.translateOnAxis(Yx,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(Ni.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?Zc.copy(t):Zc.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Ea.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?Ni.lookAt(Ea,Zc,this.up):Ni.lookAt(Zc,Ea,this.up),this.quaternion.setFromRotationMatrix(Ni),s&&(Ni.extractRotation(s.matrixWorld),js.setFromRotationMatrix(Ni),this.quaternion.premultiply(js.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(console.error("THREE.Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(Zx),$s.child=t,this.dispatchEvent($s),$s.child=null):console.error("THREE.Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(e1),Dp.child=t,this.dispatchEvent(Dp),Dp.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),Ni.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),Ni.multiply(t.parent.matrixWorld)),t.applyMatrix4(Ni),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(Zx),$s.child=t,this.dispatchEvent($s),$s.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ea,t,JI),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Ea,QI,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale),this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.6,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.visibility=this._visibility,s.active=this._active,s.bounds=this._bounds.map(l=>({boxInitialized:l.boxInitialized,boxMin:l.box.min.toArray(),boxMax:l.box.max.toArray(),sphereInitialized:l.sphereInitialized,sphereRadius:l.sphere.radius,sphereCenter:l.sphere.center.toArray()})),s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.geometryCount=this._geometryCount,s.matricesTexture=this._matricesTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere={center:s.boundingSphere.center.toArray(),radius:s.boundingSphere.radius}),this.boundingBox!==null&&(s.boundingBox={min:s.boundingBox.min.toArray(),max:s.boundingBox.max.toArray()}));function o(l,c){return l[c.uuid]===void 0&&(l[c.uuid]=c.toJSON(t)),c.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let l=this.geometry.parameters;if(l!==void 0&&l.shapes!==void 0){let c=l.shapes;if(Array.isArray(c))for(let u=0,d=c.length;u<d;u++){let h=c[u];o(t.shapes,h)}else o(t.shapes,c)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let l=[];for(let c=0,u=this.material.length;c<u;c++)l.push(o(t.materials,this.material[c]));s.material=l}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let l=0;l<this.children.length;l++)s.children.push(this.children[l].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let l=0;l<this.animations.length;l++){let c=this.animations[l];s.animations.push(o(t.animations,c))}}if(i){let l=a(t.geometries),c=a(t.materials),u=a(t.textures),d=a(t.images),h=a(t.shapes),f=a(t.skeletons),m=a(t.animations),v=a(t.nodes);l.length>0&&(r.geometries=l),c.length>0&&(r.materials=c),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),h.length>0&&(r.shapes=h),f.length>0&&(r.skeletons=f),m.length>0&&(r.animations=m),v.length>0&&(r.nodes=v)}return r.object=s,r;function a(l){let c=[];for(let u in l){let d=l[u];delete d.metadata,c.push(d)}return c}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new R(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Gn=new R,Pi=new R,Ap=new R,Oi=new R,qs=new R,Xs=new R,Kx=new R,Ip=new R,Rp=new R,Np=new R,Pp=new At,Op=new At,Lp=new At,lr=class n{constructor(e=new R,t=new R,i=new R){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Gn.subVectors(e,t),r.cross(Gn);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Gn.subVectors(r,t),Pi.subVectors(i,t),Ap.subVectors(e,t);let o=Gn.dot(Gn),a=Gn.dot(Pi),l=Gn.dot(Ap),c=Pi.dot(Pi),u=Pi.dot(Ap),d=o*c-a*a;if(d===0)return s.set(0,0,0),null;let h=1/d,f=(c*l-a*u)*h,m=(o*u-a*l)*h;return s.set(1-f-m,m,f)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,Oi)===null?!1:Oi.x>=0&&Oi.y>=0&&Oi.x+Oi.y<=1}static getInterpolation(e,t,i,r,s,o,a,l){return this.getBarycoord(e,t,i,r,Oi)===null?(l.x=0,l.y=0,"z"in l&&(l.z=0),"w"in l&&(l.w=0),null):(l.setScalar(0),l.addScaledVector(s,Oi.x),l.addScaledVector(o,Oi.y),l.addScaledVector(a,Oi.z),l)}static getInterpolatedAttribute(e,t,i,r,s,o){return Pp.setScalar(0),Op.setScalar(0),Lp.setScalar(0),Pp.fromBufferAttribute(e,t),Op.fromBufferAttribute(e,i),Lp.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(Pp,s.x),o.addScaledVector(Op,s.y),o.addScaledVector(Lp,s.z),o}static isFrontFacing(e,t,i,r){return Gn.subVectors(i,t),Pi.subVectors(e,t),Gn.cross(Pi).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Gn.subVectors(this.c,this.b),Pi.subVectors(this.a,this.b),Gn.cross(Pi).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;qs.subVectors(r,i),Xs.subVectors(s,i),Ip.subVectors(e,i);let l=qs.dot(Ip),c=Xs.dot(Ip);if(l<=0&&c<=0)return t.copy(i);Rp.subVectors(e,r);let u=qs.dot(Rp),d=Xs.dot(Rp);if(u>=0&&d<=u)return t.copy(r);let h=l*d-u*c;if(h<=0&&l>=0&&u<=0)return o=l/(l-u),t.copy(i).addScaledVector(qs,o);Np.subVectors(e,s);let f=qs.dot(Np),m=Xs.dot(Np);if(m>=0&&f<=m)return t.copy(s);let v=f*c-l*m;if(v<=0&&c>=0&&m<=0)return a=c/(c-m),t.copy(i).addScaledVector(Xs,a);let g=u*m-f*d;if(g<=0&&d-u>=0&&f-m>=0)return Kx.subVectors(s,r),a=(d-u)/(d-u+(f-m)),t.copy(r).addScaledVector(Kx,a);let p=1/(g+v+h);return o=v*p,a=h*p,t.copy(i).addScaledVector(qs,o).addScaledVector(Xs,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},QM={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},sr={h:0,s:0,l:0},Kc={h:0,s:0,l:0};function Fp(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var xe=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=rn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,Xe.toWorkingColorSpace(this,t),this}setRGB(e,t,i,r=Xe.workingColorSpace){return this.r=e,this.g=t,this.b=i,Xe.toWorkingColorSpace(this,r),this}setHSL(e,t,i,r=Xe.workingColorSpace){if(e=Lg(e,1),t=Bt(t,0,1),i=Bt(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=Fp(o,s,e+1/3),this.g=Fp(o,s,e),this.b=Fp(o,s,e-1/3)}return Xe.toWorkingColorSpace(this,r),this}setStyle(e,t=rn){function i(s){s!==void 0&&parseFloat(s)<1&&console.warn("THREE.Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:console.warn("THREE.Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);console.warn("THREE.Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=rn){let i=QM[e.toLowerCase()];return i!==void 0?this.setHex(i,t):console.warn("THREE.Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=ki(e.r),this.g=ki(e.g),this.b=ki(e.b),this}copyLinearToSRGB(e){return this.r=lo(e.r),this.g=lo(e.g),this.b=lo(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=rn){return Xe.fromWorkingColorSpace(Xt.copy(this),e),Math.round(Bt(Xt.r*255,0,255))*65536+Math.round(Bt(Xt.g*255,0,255))*256+Math.round(Bt(Xt.b*255,0,255))}getHexString(e=rn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=Xe.workingColorSpace){Xe.fromWorkingColorSpace(Xt.copy(this),t);let i=Xt.r,r=Xt.g,s=Xt.b,o=Math.max(i,r,s),a=Math.min(i,r,s),l,c,u=(a+o)/2;if(a===o)l=0,c=0;else{let d=o-a;switch(c=u<=.5?d/(o+a):d/(2-o-a),o){case i:l=(r-s)/d+(r<s?6:0);break;case r:l=(s-i)/d+2;break;case s:l=(i-r)/d+4;break}l/=6}return e.h=l,e.s=c,e.l=u,e}getRGB(e,t=Xe.workingColorSpace){return Xe.fromWorkingColorSpace(Xt.copy(this),t),e.r=Xt.r,e.g=Xt.g,e.b=Xt.b,e}getStyle(e=rn){Xe.fromWorkingColorSpace(Xt.copy(this),e);let t=Xt.r,i=Xt.g,r=Xt.b;return e!==rn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(sr),this.setHSL(sr.h+e,sr.s+t,sr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(sr),e.getHSL(Kc);let i=Na(sr.h,Kc.h,t),r=Na(sr.s,Kc.s,t),s=Na(sr.l,Kc.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},Xt=new xe;xe.NAMES=QM;var t1=0,Hi=class extends Vi{static get type(){return"Material"}get type(){return this.constructor.type}set type(e){}constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:t1++}),this.uuid=mi(),this.name="",this.blending=oo,this.side=cr,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Kp,this.blendDst=Jp,this.blendEquation=Br,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new xe(0,0,0),this.blendAlpha=0,this.depthFunc=uo,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=Ox,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Vs,this.stencilZFail=Vs,this.stencilZPass=Vs,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){console.warn(`THREE.Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){console.warn(`THREE.Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.6,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==oo&&(i.blending=this.blending),this.side!==cr&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Kp&&(i.blendSrc=this.blendSrc),this.blendDst!==Jp&&(i.blendDst=this.blendDst),this.blendEquation!==Br&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==uo&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==Ox&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Vs&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Vs&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Vs&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let l=s[a];delete l.metadata,o.push(l)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}onBuild(){console.warn("Material: onBuild() has been removed.")}},go=class extends Hi{static get type(){return"MeshBasicMaterial"}constructor(e){super(),this.isMeshBasicMaterial=!0,this.color=new xe(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new $r,this.combine=xg,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}};var Rt=new R,Jc=new te,Zt=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=km,this.updateRanges=[],this.gpuType=di,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Jc.fromBufferAttribute(this,t),Jc.applyMatrix3(e),this.setXY(t,Jc.x,Jc.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Rt.fromBufferAttribute(this,t),Rt.applyMatrix3(e),this.setXYZ(t,Rt.x,Rt.y,Rt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Rt.fromBufferAttribute(this,t),Rt.applyMatrix4(e),this.setXYZ(t,Rt.x,Rt.y,Rt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Rt.fromBufferAttribute(this,t),Rt.applyNormalMatrix(e),this.setXYZ(t,Rt.x,Rt.y,Rt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Rt.fromBufferAttribute(this,t),Rt.transformDirection(e),this.setXYZ(t,Rt.x,Rt.y,Rt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=jn(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=ut(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=jn(t,this.array)),t}setX(e,t){return this.normalized&&(t=ut(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=jn(t,this.array)),t}setY(e,t){return this.normalized&&(t=ut(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=jn(t,this.array)),t}setZ(e,t){return this.normalized&&(t=ut(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=jn(t,this.array)),t}setW(e,t){return this.normalized&&(t=ut(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=ut(t,this.array),i=ut(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=ut(t,this.array),i=ut(i,this.array),r=ut(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=ut(t,this.array),i=ut(i,this.array),r=ut(r,this.array),s=ut(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==km&&(e.usage=this.usage),e}};var Au=class extends Zt{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Iu=class extends Zt{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var dt=class extends Zt{constructor(e,t,i){super(new Float32Array(e),t,i)}},n1=0,Nn=new tt,kp=new qn,Ys=new R,yn=new Bi,ba=new Bi,Vt=new R,zt=class n extends Vi{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:n1++}),this.uuid=mi(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(JM(e)?Iu:Au)(e,1):this.index=e,this}setIndirect(e){return this.indirect=e,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Fe().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return Nn.makeRotationFromQuaternion(e),this.applyMatrix4(Nn),this}rotateX(e){return Nn.makeRotationX(e),this.applyMatrix4(Nn),this}rotateY(e){return Nn.makeRotationY(e),this.applyMatrix4(Nn),this}rotateZ(e){return Nn.makeRotationZ(e),this.applyMatrix4(Nn),this}translate(e,t,i){return Nn.makeTranslation(e,t,i),this.applyMatrix4(Nn),this}scale(e,t,i){return Nn.makeScale(e,t,i),this.applyMatrix4(Nn),this}lookAt(e){return kp.lookAt(e),kp.updateMatrix(),this.applyMatrix4(kp.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(Ys).negate(),this.translate(Ys.x,Ys.y,Ys.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new dt(i,3))}else{for(let i=0,r=t.count;i<r;i++){let s=e[i];t.setXYZ(i,s.x,s.y,s.z||0)}e.length>t.count&&console.warn("THREE.BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Bi);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new R(-1/0,-1/0,-1/0),new R(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];yn.setFromBufferAttribute(s),this.morphTargetsRelative?(Vt.addVectors(this.boundingBox.min,yn.min),this.boundingBox.expandByPoint(Vt),Vt.addVectors(this.boundingBox.max,yn.max),this.boundingBox.expandByPoint(Vt)):(this.boundingBox.expandByPoint(yn.min),this.boundingBox.expandByPoint(yn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&console.error('THREE.BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new jr);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){console.error("THREE.BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new R,1/0);return}if(e){let i=this.boundingSphere.center;if(yn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];ba.setFromBufferAttribute(a),this.morphTargetsRelative?(Vt.addVectors(yn.min,ba.min),yn.expandByPoint(Vt),Vt.addVectors(yn.max,ba.max),yn.expandByPoint(Vt)):(yn.expandByPoint(ba.min),yn.expandByPoint(ba.max))}yn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Vt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Vt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],l=this.morphTargetsRelative;for(let c=0,u=a.count;c<u;c++)Vt.fromBufferAttribute(a,c),l&&(Ys.fromBufferAttribute(e,c),Vt.add(Ys)),r=Math.max(r,i.distanceToSquared(Vt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&console.error('THREE.BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){console.error("THREE.BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new Zt(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],l=[];for(let I=0;I<i.count;I++)a[I]=new R,l[I]=new R;let c=new R,u=new R,d=new R,h=new te,f=new te,m=new te,v=new R,g=new R;function p(I,b,x){c.fromBufferAttribute(i,I),u.fromBufferAttribute(i,b),d.fromBufferAttribute(i,x),h.fromBufferAttribute(s,I),f.fromBufferAttribute(s,b),m.fromBufferAttribute(s,x),u.sub(c),d.sub(c),f.sub(h),m.sub(h);let D=1/(f.x*m.y-m.x*f.y);isFinite(D)&&(v.copy(u).multiplyScalar(m.y).addScaledVector(d,-f.y).multiplyScalar(D),g.copy(d).multiplyScalar(f.x).addScaledVector(u,-m.x).multiplyScalar(D),a[I].add(v),a[b].add(v),a[x].add(v),l[I].add(g),l[b].add(g),l[x].add(g))}let M=this.groups;M.length===0&&(M=[{start:0,count:e.count}]);for(let I=0,b=M.length;I<b;++I){let x=M[I],D=x.start,N=x.count;for(let O=D,U=D+N;O<U;O+=3)p(e.getX(O+0),e.getX(O+1),e.getX(O+2))}let _=new R,y=new R,A=new R,C=new R;function S(I){A.fromBufferAttribute(r,I),C.copy(A);let b=a[I];_.copy(b),_.sub(A.multiplyScalar(A.dot(b))).normalize(),y.crossVectors(C,b);let D=y.dot(l[I])<0?-1:1;o.setXYZW(I,_.x,_.y,_.z,D)}for(let I=0,b=M.length;I<b;++I){let x=M[I],D=x.start,N=x.count;for(let O=D,U=D+N;O<U;O+=3)S(e.getX(O+0)),S(e.getX(O+1)),S(e.getX(O+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new Zt(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let h=0,f=i.count;h<f;h++)i.setXYZ(h,0,0,0);let r=new R,s=new R,o=new R,a=new R,l=new R,c=new R,u=new R,d=new R;if(e)for(let h=0,f=e.count;h<f;h+=3){let m=e.getX(h+0),v=e.getX(h+1),g=e.getX(h+2);r.fromBufferAttribute(t,m),s.fromBufferAttribute(t,v),o.fromBufferAttribute(t,g),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,m),l.fromBufferAttribute(i,v),c.fromBufferAttribute(i,g),a.add(u),l.add(u),c.add(u),i.setXYZ(m,a.x,a.y,a.z),i.setXYZ(v,l.x,l.y,l.z),i.setXYZ(g,c.x,c.y,c.z)}else for(let h=0,f=t.count;h<f;h+=3)r.fromBufferAttribute(t,h+0),s.fromBufferAttribute(t,h+1),o.fromBufferAttribute(t,h+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(h+0,u.x,u.y,u.z),i.setXYZ(h+1,u.x,u.y,u.z),i.setXYZ(h+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Vt.fromBufferAttribute(e,t),Vt.normalize(),e.setXYZ(t,Vt.x,Vt.y,Vt.z)}toNonIndexed(){function e(a,l){let c=a.array,u=a.itemSize,d=a.normalized,h=new c.constructor(l.length*u),f=0,m=0;for(let v=0,g=l.length;v<g;v++){a.isInterleavedBufferAttribute?f=l[v]*a.data.stride+a.offset:f=l[v]*u;for(let p=0;p<u;p++)h[m++]=c[f++]}return new Zt(h,u,d)}if(this.index===null)return console.warn("THREE.BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let l=r[a],c=e(l,i);t.setAttribute(a,c)}let s=this.morphAttributes;for(let a in s){let l=[],c=s[a];for(let u=0,d=c.length;u<d;u++){let h=c[u],f=e(h,i);l.push(f)}t.morphAttributes[a]=l}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,l=o.length;a<l;a++){let c=o[a];t.addGroup(c.start,c.count,c.materialIndex)}return t}toJSON(){let e={metadata:{version:4.6,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let l=this.parameters;for(let c in l)l[c]!==void 0&&(e[c]=l[c]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let l in i){let c=i[l];e.data.attributes[l]=c.toJSON(e.data)}let r={},s=!1;for(let l in this.morphAttributes){let c=this.morphAttributes[l],u=[];for(let d=0,h=c.length;d<h;d++){let f=c[d];u.push(f.toJSON(e.data))}u.length>0&&(r[l]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere={center:a.center.toArray(),radius:a.radius}),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone(t));let r=e.attributes;for(let c in r){let u=r[c];this.setAttribute(c,u.clone(t))}let s=e.morphAttributes;for(let c in s){let u=[],d=s[c];for(let h=0,f=d.length;h<f;h++)u.push(d[h].clone(t));this.morphAttributes[c]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let c=0,u=o.length;c<u;c++){let d=o[c];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let l=e.boundingSphere;return l!==null&&(this.boundingSphere=l.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}},Jx=new tt,Lr=new Va,Qc=new jr,Qx=new R,eu=new R,tu=new R,nu=new R,Up=new R,iu=new R,eM=new R,ru=new R,wt=class extends qn{constructor(e=new zt,t=new go){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){iu.set(0,0,0);for(let l=0,c=s.length;l<c;l++){let u=a[l],d=s[l];u!==0&&(Up.fromBufferAttribute(d,e),o?iu.addScaledVector(Up,u):iu.addScaledVector(Up.sub(t),u))}t.add(iu)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Qc.copy(i.boundingSphere),Qc.applyMatrix4(s),Lr.copy(e.ray).recast(e.near),!(Qc.containsPoint(Lr.origin)===!1&&(Lr.intersectSphere(Qc,Qx)===null||Lr.origin.distanceToSquared(Qx)>(e.far-e.near)**2))&&(Jx.copy(s).invert(),Lr.copy(e.ray).applyMatrix4(Jx),!(i.boundingBox!==null&&Lr.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,Lr)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,l=s.attributes.position,c=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,h=s.groups,f=s.drawRange;if(a!==null)if(Array.isArray(o))for(let m=0,v=h.length;m<v;m++){let g=h[m],p=o[g.materialIndex],M=Math.max(g.start,f.start),_=Math.min(a.count,Math.min(g.start+g.count,f.start+f.count));for(let y=M,A=_;y<A;y+=3){let C=a.getX(y),S=a.getX(y+1),I=a.getX(y+2);r=su(this,p,e,i,c,u,d,C,S,I),r&&(r.faceIndex=Math.floor(y/3),r.face.materialIndex=g.materialIndex,t.push(r))}}else{let m=Math.max(0,f.start),v=Math.min(a.count,f.start+f.count);for(let g=m,p=v;g<p;g+=3){let M=a.getX(g),_=a.getX(g+1),y=a.getX(g+2);r=su(this,o,e,i,c,u,d,M,_,y),r&&(r.faceIndex=Math.floor(g/3),t.push(r))}}else if(l!==void 0)if(Array.isArray(o))for(let m=0,v=h.length;m<v;m++){let g=h[m],p=o[g.materialIndex],M=Math.max(g.start,f.start),_=Math.min(l.count,Math.min(g.start+g.count,f.start+f.count));for(let y=M,A=_;y<A;y+=3){let C=y,S=y+1,I=y+2;r=su(this,p,e,i,c,u,d,C,S,I),r&&(r.faceIndex=Math.floor(y/3),r.face.materialIndex=g.materialIndex,t.push(r))}}else{let m=Math.max(0,f.start),v=Math.min(l.count,f.start+f.count);for(let g=m,p=v;g<p;g+=3){let M=g,_=g+1,y=g+2;r=su(this,o,e,i,c,u,d,M,_,y),r&&(r.faceIndex=Math.floor(g/3),t.push(r))}}}};function i1(n,e,t,i,r,s,o,a){let l;if(e.side===Yt?l=i.intersectTriangle(o,s,r,!0,a):l=i.intersectTriangle(r,s,o,e.side===cr,a),l===null)return null;ru.copy(a),ru.applyMatrix4(n.matrixWorld);let c=t.ray.origin.distanceTo(ru);return c<t.near||c>t.far?null:{distance:c,point:ru.clone(),object:n}}function su(n,e,t,i,r,s,o,a,l,c){n.getVertexPosition(a,eu),n.getVertexPosition(l,tu),n.getVertexPosition(c,nu);let u=i1(n,e,t,i,eu,tu,nu,eM);if(u){let d=new R;lr.getBarycoord(eM,eu,tu,nu,d),r&&(u.uv=lr.getInterpolatedAttribute(r,a,l,c,d,new te)),s&&(u.uv1=lr.getInterpolatedAttribute(s,a,l,c,d,new te)),o&&(u.normal=lr.getInterpolatedAttribute(o,a,l,c,d,new R),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let h={a,b:l,c,normal:new R,materialIndex:0};lr.getNormal(eu,tu,nu,h.normal),u.face=h,u.barycoord=d}return u}var qr=class n extends zt{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let l=[],c=[],u=[],d=[],h=0,f=0;m("z","y","x",-1,-1,i,t,e,o,s,0),m("z","y","x",1,-1,i,t,-e,o,s,1),m("x","z","y",1,1,e,i,t,r,o,2),m("x","z","y",1,-1,e,i,-t,r,o,3),m("x","y","z",1,-1,e,t,i,r,s,4),m("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(l),this.setAttribute("position",new dt(c,3)),this.setAttribute("normal",new dt(u,3)),this.setAttribute("uv",new dt(d,2));function m(v,g,p,M,_,y,A,C,S,I,b){let x=y/S,D=A/I,N=y/2,O=A/2,U=C/2,$=S+1,W=I+1,q=0,z=0,Q=new R;for(let ee=0;ee<W;ee++){let oe=ee*D-O;for(let ge=0;ge<$;ge++){let we=ge*x-N;Q[v]=we*M,Q[g]=oe*_,Q[p]=U,c.push(Q.x,Q.y,Q.z),Q[v]=0,Q[g]=0,Q[p]=C>0?1:-1,u.push(Q.x,Q.y,Q.z),d.push(ge/S),d.push(1-ee/I),q+=1}}for(let ee=0;ee<I;ee++)for(let oe=0;oe<S;oe++){let ge=h+oe+$*ee,we=h+oe+$*(ee+1),G=h+(oe+1)+$*(ee+1),J=h+(oe+1)+$*ee;l.push(ge,we,J),l.push(we,G,J),z+=6}a.addGroup(f,z,b),f+=z,h+=q}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};function yo(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(console.warn("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function nn(n){let e={};for(let t=0;t<n.length;t++){let i=yo(n[t]);for(let r in i)e[r]=i[r]}return e}function r1(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function eE(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:Xe.workingColorSpace}var Za={clone:yo,merge:nn},s1=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,o1=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,Gt=class extends Hi{static get type(){return"ShaderMaterial"}constructor(e){super(),this.isShaderMaterial=!0,this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=s1,this.fragmentShader=o1,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=yo(e.uniforms),this.uniformsGroups=r1(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Ru=class extends qn{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new tt,this.projectionMatrix=new tt,this.projectionMatrixInverse=new tt,this.coordinateSystem=Fi}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorldInverse.copy(this.matrixWorld).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorldInverse.copy(this.matrixWorld).invert()}clone(){return new this.constructor().copy(this)}},or=new R,tM=new te,nM=new te,sn=class extends Ru{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=Ua*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(Ra*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return Ua*2*Math.atan(Math.tan(Ra*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){or.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(or.x,or.y).multiplyScalar(-e/or.z),or.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(or.x,or.y).multiplyScalar(-e/or.z)}getViewSize(e,t){return this.getViewBounds(e,tM,nM),t.subVectors(nM,tM)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(Ra*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let l=o.fullWidth,c=o.fullHeight;s+=o.offsetX*r/l,t-=o.offsetY*i/c,r*=o.width/l,i*=o.height/c}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}},Zs=-90,Ks=1,Hm=class extends qn{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new sn(Zs,Ks,e,t);r.layers=this.layers,this.add(r);let s=new sn(Zs,Ks,e,t);s.layers=this.layers,this.add(s);let o=new sn(Zs,Ks,e,t);o.layers=this.layers,this.add(o);let a=new sn(Zs,Ks,e,t);a.layers=this.layers,this.add(a);let l=new sn(Zs,Ks,e,t);l.layers=this.layers,this.add(l);let c=new sn(Zs,Ks,e,t);c.layers=this.layers,this.add(c)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,l]=t;for(let c of t)this.remove(c);if(e===Fi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),l.up.set(0,1,0),l.lookAt(0,0,-1);else if(e===wu)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),l.up.set(0,-1,0),l.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let c of t)this.add(c),c.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,l,c,u]=this.children,d=e.getRenderTarget(),h=e.getActiveCubeFace(),f=e.getActiveMipmapLevel(),m=e.xr.enabled;e.xr.enabled=!1;let v=i.texture.generateMipmaps;i.texture.generateMipmaps=!1,e.setRenderTarget(i,0,r),e.render(t,s),e.setRenderTarget(i,1,r),e.render(t,o),e.setRenderTarget(i,2,r),e.render(t,a),e.setRenderTarget(i,3,r),e.render(t,l),e.setRenderTarget(i,4,r),e.render(t,c),i.texture.generateMipmaps=v,e.setRenderTarget(i,5,r),e.render(t,u),e.setRenderTarget(d,h,f),e.xr.enabled=m,i.texture.needsPMREMUpdate=!0}},Nu=class extends zi{constructor(e,t,i,r,s,o,a,l,c,u){e=e!==void 0?e:[],t=t!==void 0?t:fo,super(e,t,i,r,s,o,a,l,c,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},zm=class extends xn{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new Nu(r,t.mapping,t.wrapS,t.wrapT,t.magFilter,t.minFilter,t.format,t.type,t.anisotropy,t.colorSpace),this.texture.isRenderTargetTexture=!0,this.texture.generateMipmaps=t.generateMipmaps!==void 0?t.generateMipmaps:!1,this.texture.minFilter=t.minFilter!==void 0?t.minFilter:vn}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new qr(5,5,5),s=new Gt({name:"CubemapFromEquirect",uniforms:yo(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:Yt,blending:hi});s.uniforms.tEquirect.value=t;let o=new wt(r,s),a=t.minFilter;return t.minFilter===Gr&&(t.minFilter=vn),new Hm(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t,i,r){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}},Vp=new R,a1=new R,l1=new Fe,Wn=class{constructor(e=new R(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=Vp.subVectors(i,t).cross(a1.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(Vp),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let s=-(e.start.dot(this.normal)+this.constant)/r;return s<0||s>1?null:t.copy(e.start).addScaledVector(i,s)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||l1.getNormalMatrix(e),r=this.coplanarPoint(Vp).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},Fr=new jr,ou=new R,Ba=class{constructor(e=new Wn,t=new Wn,i=new Wn,r=new Wn,s=new Wn,o=new Wn){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=Fi){let i=this.planes,r=e.elements,s=r[0],o=r[1],a=r[2],l=r[3],c=r[4],u=r[5],d=r[6],h=r[7],f=r[8],m=r[9],v=r[10],g=r[11],p=r[12],M=r[13],_=r[14],y=r[15];if(i[0].setComponents(l-s,h-c,g-f,y-p).normalize(),i[1].setComponents(l+s,h+c,g+f,y+p).normalize(),i[2].setComponents(l+o,h+u,g+m,y+M).normalize(),i[3].setComponents(l-o,h-u,g-m,y-M).normalize(),i[4].setComponents(l-a,h-d,g-v,y-_).normalize(),t===Fi)i[5].setComponents(l+a,h+d,g+v,y+_).normalize();else if(t===wu)i[5].setComponents(a,d,v,_).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),Fr.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),Fr.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(Fr)}intersectsSprite(e){return Fr.center.set(0,0,0),Fr.radius=.7071067811865476,Fr.applyMatrix4(e.matrixWorld),this.intersectsSphere(Fr)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(ou.x=r.normal.x>0?e.max.x:e.min.x,ou.y=r.normal.y>0?e.max.y:e.min.y,ou.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(ou)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};function tE(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function c1(n){let e=new WeakMap;function t(a,l){let c=a.array,u=a.usage,d=c.byteLength,h=n.createBuffer();n.bindBuffer(l,h),n.bufferData(l,c,u),a.onUploadCallback();let f;if(c instanceof Float32Array)f=n.FLOAT;else if(c instanceof Uint16Array)a.isFloat16BufferAttribute?f=n.HALF_FLOAT:f=n.UNSIGNED_SHORT;else if(c instanceof Int16Array)f=n.SHORT;else if(c instanceof Uint32Array)f=n.UNSIGNED_INT;else if(c instanceof Int32Array)f=n.INT;else if(c instanceof Int8Array)f=n.BYTE;else if(c instanceof Uint8Array)f=n.UNSIGNED_BYTE;else if(c instanceof Uint8ClampedArray)f=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+c);return{buffer:h,type:f,bytesPerElement:c.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,l,c){let u=l.array,d=l.updateRanges;if(n.bindBuffer(c,a),d.length===0)n.bufferSubData(c,0,u);else{d.sort((f,m)=>f.start-m.start);let h=0;for(let f=1;f<d.length;f++){let m=d[h],v=d[f];v.start<=m.start+m.count+1?m.count=Math.max(m.count,v.start+v.count-m.start):(++h,d[h]=v)}d.length=h+1;for(let f=0,m=d.length;f<m;f++){let v=d[f];n.bufferSubData(c,v.start*u.BYTES_PER_ELEMENT,u,v.start,v.count)}l.clearUpdateRanges()}l.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let l=e.get(a);l&&(n.deleteBuffer(l.buffer),e.delete(a))}function o(a,l){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let c=e.get(a);if(c===void 0)e.set(a,t(a,l));else if(c.version<a.version){if(c.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(c.buffer,a,l),c.version=a.version}}return{get:r,remove:s,update:o}}var Xr=class n extends zt{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),l=Math.floor(r),c=a+1,u=l+1,d=e/a,h=t/l,f=[],m=[],v=[],g=[];for(let p=0;p<u;p++){let M=p*h-o;for(let _=0;_<c;_++){let y=_*d-s;m.push(y,-M,0),v.push(0,0,1),g.push(_/a),g.push(1-p/l)}}for(let p=0;p<l;p++)for(let M=0;M<a;M++){let _=M+c*p,y=M+c*(p+1),A=M+1+c*(p+1),C=M+1+c*p;f.push(_,y,C),f.push(y,A,C)}this.setIndex(f),this.setAttribute("position",new dt(m,3)),this.setAttribute("normal",new dt(v,3)),this.setAttribute("uv",new dt(g,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}},u1=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,d1=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,f1=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,h1=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,p1=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,m1=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,g1=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,y1=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,v1=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec3 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 ).rgb;
	}
#endif`,_1=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,x1=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,M1=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,E1=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,b1=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,w1=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,S1=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,C1=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,T1=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,D1=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,A1=`#if defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#elif defined( USE_COLOR )
	diffuseColor.rgb *= vColor;
#endif`,I1=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR )
	varying vec3 vColor;
#endif`,R1=`#if defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec3 vColor;
#endif`,N1=`#if defined( USE_COLOR_ALPHA )
	vColor = vec4( 1.0 );
#elif defined( USE_COLOR ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec3( 1.0 );
#endif
#ifdef USE_COLOR
	vColor *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.xyz *= instanceColor.xyz;
#endif
#ifdef USE_BATCHING_COLOR
	vec3 batchingColor = getBatchingColor( getIndirectIndex( gl_DrawID ) );
	vColor.xyz *= batchingColor.xyz;
#endif`,P1=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
mat3 transposeMat3( const in mat3 m ) {
	mat3 tmp;
	tmp[ 0 ] = vec3( m[ 0 ].x, m[ 1 ].x, m[ 2 ].x );
	tmp[ 1 ] = vec3( m[ 0 ].y, m[ 1 ].y, m[ 2 ].y );
	tmp[ 2 ] = vec3( m[ 0 ].z, m[ 1 ].z, m[ 2 ].z );
	return tmp;
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,O1=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,L1=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,F1=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,k1=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,U1=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,V1=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,B1="gl_FragColor = linearToOutputTexel( gl_FragColor );",H1=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,z1=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * vec3( flipEnvMap * reflectVec.x, reflectVec.yz ) );
	#else
		vec4 envColor = vec4( 0.0 );
	#endif
	#ifdef ENVMAP_BLENDING_MULTIPLY
		outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
	#elif defined( ENVMAP_BLENDING_MIX )
		outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
	#elif defined( ENVMAP_BLENDING_ADD )
		outgoingLight += envColor.xyz * specularStrength * reflectivity;
	#endif
#endif`,G1=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
	
#endif`,W1=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,j1=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,$1=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,q1=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,X1=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,Y1=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,Z1=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,K1=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,J1=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,Q1=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,eR=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,tR=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif`,nR=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, roughness * roughness) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,iR=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,rR=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,sR=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,oR=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,aR=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb * ( 1.0 - metalnessFactor );
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = mix( min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = mix( vec3( 0.04 ), diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.07, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,lR=`struct PhysicalMaterial {
	vec3 diffuseColor;
	float roughness;
	vec3 specularColor;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		float v = 0.5 / ( gv + gl );
		return saturate(v);
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColor;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transposeMat3( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float a = roughness < 0.25 ? -339.2 * r2 + 161.4 * roughness - 25.9 : -8.48 * r2 + 14.3 * roughness - 9.95;
	float b = roughness < 0.25 ? 44.0 * r2 - 23.7 * roughness + 3.26 : 1.97 * r2 - 3.27 * roughness + 0.72;
	float DG = exp( a * dotNV + b ) + ( roughness < 0.25 ? 0.0 : 0.1 * ( roughness - 0.25 ) );
	return saturate( DG * RECIPROCAL_PI );
}
vec2 DFGApprox( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	const vec4 c0 = vec4( - 1, - 0.0275, - 0.572, 0.022 );
	const vec4 c1 = vec4( 1, 0.0425, 1.04, - 0.04 );
	vec4 r = roughness * c0 + c1;
	float a004 = min( r.x * r.x, exp2( - 9.28 * dotNV ) ) * r.x + r.y;
	vec2 fab = vec2( - 1.04, 1.04 ) * a004 + r.zw;
	return fab;
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	vec2 fab = DFGApprox( normal, viewDir, roughness );
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	vec2 fab = DFGApprox( normal, viewDir, roughness );
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColor * t2.x + ( vec3( 1.0 ) - material.specularColor ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseColor * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
	#endif
	vec3 singleScattering = vec3( 0.0 );
	vec3 multiScattering = vec3( 0.0 );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnel, material.roughness, singleScattering, multiScattering );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScattering, multiScattering );
	#endif
	vec3 totalScattering = singleScattering + multiScattering;
	vec3 diffuse = material.diffuseColor * ( 1.0 - max( max( totalScattering.r, totalScattering.g ), totalScattering.b ) );
	reflectedLight.indirectSpecular += radiance * singleScattering;
	reflectedLight.indirectSpecular += multiScattering * cosineWeightedIrradiance;
	reflectedLight.indirectDiffuse += diffuse * cosineWeightedIrradiance;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,cR=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnel = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,uR=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD ) && defined( ENVMAP_TYPE_CUBE_UV )
		iblIrradiance += getIBLIrradiance( geometryNormal );
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,dR=`#if defined( RE_IndirectDiffuse )
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,fR=`#if defined( USE_LOGDEPTHBUF )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,hR=`#if defined( USE_LOGDEPTHBUF )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,pR=`#ifdef USE_LOGDEPTHBUF
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,mR=`#ifdef USE_LOGDEPTHBUF
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,gR=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,yR=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,vR=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,_R=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,xR=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,MR=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,ER=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,bR=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,wR=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,SR=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,CR=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,TR=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,DR=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,AR=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,IR=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,RR=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,NR=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,PR=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,OR=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,LR=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,FR=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,kR=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,UR=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	return depth * ( near - far ) - near;
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	return ( near * far ) / ( ( far - near ) * depth - far );
}`,VR=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,BR=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,HR=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,zR=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,GR=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,WR=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,jR=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform sampler2D pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	float texture2DCompare( sampler2D depths, vec2 uv, float compare ) {
		return step( compare, unpackRGBAToDepth( texture2D( depths, uv ) ) );
	}
	vec2 texture2DDistribution( sampler2D shadow, vec2 uv ) {
		return unpackRGBATo2Half( texture2D( shadow, uv ) );
	}
	float VSMShadow (sampler2D shadow, vec2 uv, float compare ){
		float occlusion = 1.0;
		vec2 distribution = texture2DDistribution( shadow, uv );
		float hard_shadow = step( compare , distribution.x );
		if (hard_shadow != 1.0 ) {
			float distance = compare - distribution.x ;
			float variance = max( 0.00000, distribution.y * distribution.y );
			float softness_probability = variance / (variance + distance * distance );			softness_probability = clamp( ( softness_probability - 0.3 ) / ( 0.95 - 0.3 ), 0.0, 1.0 );			occlusion = clamp( max( hard_shadow, softness_probability ), 0.0, 1.0 );
		}
		return occlusion;
	}
	float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
		float shadow = 1.0;
		shadowCoord.xyz /= shadowCoord.w;
		shadowCoord.z += shadowBias;
		bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
		bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
		if ( frustumTest ) {
		#if defined( SHADOWMAP_TYPE_PCF )
			vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
			float dx0 = - texelSize.x * shadowRadius;
			float dy0 = - texelSize.y * shadowRadius;
			float dx1 = + texelSize.x * shadowRadius;
			float dy1 = + texelSize.y * shadowRadius;
			float dx2 = dx0 / 2.0;
			float dy2 = dy0 / 2.0;
			float dx3 = dx1 / 2.0;
			float dy3 = dy1 / 2.0;
			shadow = (
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, dy0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, dy2 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy, shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx2, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx3, dy3 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx0, dy1 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( 0.0, dy1 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, shadowCoord.xy + vec2( dx1, dy1 ), shadowCoord.z )
			) * ( 1.0 / 17.0 );
		#elif defined( SHADOWMAP_TYPE_PCF_SOFT )
			vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
			float dx = texelSize.x;
			float dy = texelSize.y;
			vec2 uv = shadowCoord.xy;
			vec2 f = fract( uv * shadowMapSize + 0.5 );
			uv -= f * texelSize;
			shadow = (
				texture2DCompare( shadowMap, uv, shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + vec2( dx, 0.0 ), shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + vec2( 0.0, dy ), shadowCoord.z ) +
				texture2DCompare( shadowMap, uv + texelSize, shadowCoord.z ) +
				mix( texture2DCompare( shadowMap, uv + vec2( -dx, 0.0 ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, 0.0 ), shadowCoord.z ),
					 f.x ) +
				mix( texture2DCompare( shadowMap, uv + vec2( -dx, dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, dy ), shadowCoord.z ),
					 f.x ) +
				mix( texture2DCompare( shadowMap, uv + vec2( 0.0, -dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( 0.0, 2.0 * dy ), shadowCoord.z ),
					 f.y ) +
				mix( texture2DCompare( shadowMap, uv + vec2( dx, -dy ), shadowCoord.z ),
					 texture2DCompare( shadowMap, uv + vec2( dx, 2.0 * dy ), shadowCoord.z ),
					 f.y ) +
				mix( mix( texture2DCompare( shadowMap, uv + vec2( -dx, -dy ), shadowCoord.z ),
						  texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, -dy ), shadowCoord.z ),
						  f.x ),
					 mix( texture2DCompare( shadowMap, uv + vec2( -dx, 2.0 * dy ), shadowCoord.z ),
						  texture2DCompare( shadowMap, uv + vec2( 2.0 * dx, 2.0 * dy ), shadowCoord.z ),
						  f.x ),
					 f.y )
			) * ( 1.0 / 9.0 );
		#elif defined( SHADOWMAP_TYPE_VSM )
			shadow = VSMShadow( shadowMap, shadowCoord.xy, shadowCoord.z );
		#else
			shadow = texture2DCompare( shadowMap, shadowCoord.xy, shadowCoord.z );
		#endif
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	vec2 cubeToUV( vec3 v, float texelSizeY ) {
		vec3 absV = abs( v );
		float scaleToCube = 1.0 / max( absV.x, max( absV.y, absV.z ) );
		absV *= scaleToCube;
		v *= scaleToCube * ( 1.0 - 2.0 * texelSizeY );
		vec2 planar = v.xy;
		float almostATexel = 1.5 * texelSizeY;
		float almostOne = 1.0 - almostATexel;
		if ( absV.z >= almostOne ) {
			if ( v.z > 0.0 )
				planar.x = 4.0 - v.x;
		} else if ( absV.x >= almostOne ) {
			float signX = sign( v.x );
			planar.x = v.z * signX + 2.0 * signX;
		} else if ( absV.y >= almostOne ) {
			float signY = sign( v.y );
			planar.x = v.x + 2.0 * signY + 2.0;
			planar.y = v.z * signY - 2.0;
		}
		return vec2( 0.125, 0.25 ) * planar + vec2( 0.375, 0.75 );
	}
	float getPointShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		
		float lightToPositionLength = length( lightToPosition );
		if ( lightToPositionLength - shadowCameraFar <= 0.0 && lightToPositionLength - shadowCameraNear >= 0.0 ) {
			float dp = ( lightToPositionLength - shadowCameraNear ) / ( shadowCameraFar - shadowCameraNear );			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			vec2 texelSize = vec2( 1.0 ) / ( shadowMapSize * vec2( 4.0, 2.0 ) );
			#if defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_PCF_SOFT ) || defined( SHADOWMAP_TYPE_VSM )
				vec2 offset = vec2( - 1, 1 ) * shadowRadius * texelSize.y;
				shadow = (
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xyy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yyy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xyx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yyx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xxy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yxy, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.xxx, texelSize.y ), dp ) +
					texture2DCompare( shadowMap, cubeToUV( bd3D + offset.yxx, texelSize.y ), dp )
				) * ( 1.0 / 9.0 );
			#else
				shadow = texture2DCompare( shadowMap, cubeToUV( bd3D, texelSize.y ), dp );
			#endif
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
#endif`,$R=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,qR=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,XR=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,YR=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,ZR=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,KR=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,JR=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,QR=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,eN=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,tN=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,nN=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,iN=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseColor, material.specularColor, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,rN=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
		
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
		
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		
		#else
		
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,sN=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,oN=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,aN=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,lN=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,cN=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,uN=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,dN=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,fN=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float flipEnvMap;
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vec3( flipEnvMap * vWorldDirection.x, vWorldDirection.yz ) );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,hN=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,pN=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,mN=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,gN=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	float fragCoordZ = 0.5 * vHighPrecisionZW[0] / vHighPrecisionZW[1] + 0.5;
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,yN=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,vN=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = packDepthToRGBA( dist );
}`,_N=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,xN=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,MN=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,EN=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,bN=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,wN=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,SN=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,CN=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,TN=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,DN=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,AN=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,IN=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <packing>
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( packNormalToRGB( normal ), diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,RN=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,NN=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,PN=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,ON=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
		float sheenEnergyComp = 1.0 - 0.157 * max3( material.sheenColor );
		outgoingLight = outgoingLight * sheenEnergyComp + sheenSpecularDirect + sheenSpecularIndirect;
	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,LN=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,FN=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <packing>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,kN=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,UN=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,VN=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,BN=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <packing>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,HN=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,zN=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,Ue={alphahash_fragment:u1,alphahash_pars_fragment:d1,alphamap_fragment:f1,alphamap_pars_fragment:h1,alphatest_fragment:p1,alphatest_pars_fragment:m1,aomap_fragment:g1,aomap_pars_fragment:y1,batching_pars_vertex:v1,batching_vertex:_1,begin_vertex:x1,beginnormal_vertex:M1,bsdfs:E1,iridescence_fragment:b1,bumpmap_pars_fragment:w1,clipping_planes_fragment:S1,clipping_planes_pars_fragment:C1,clipping_planes_pars_vertex:T1,clipping_planes_vertex:D1,color_fragment:A1,color_pars_fragment:I1,color_pars_vertex:R1,color_vertex:N1,common:P1,cube_uv_reflection_fragment:O1,defaultnormal_vertex:L1,displacementmap_pars_vertex:F1,displacementmap_vertex:k1,emissivemap_fragment:U1,emissivemap_pars_fragment:V1,colorspace_fragment:B1,colorspace_pars_fragment:H1,envmap_fragment:z1,envmap_common_pars_fragment:G1,envmap_pars_fragment:W1,envmap_pars_vertex:j1,envmap_physical_pars_fragment:nR,envmap_vertex:$1,fog_vertex:q1,fog_pars_vertex:X1,fog_fragment:Y1,fog_pars_fragment:Z1,gradientmap_pars_fragment:K1,lightmap_pars_fragment:J1,lights_lambert_fragment:Q1,lights_lambert_pars_fragment:eR,lights_pars_begin:tR,lights_toon_fragment:iR,lights_toon_pars_fragment:rR,lights_phong_fragment:sR,lights_phong_pars_fragment:oR,lights_physical_fragment:aR,lights_physical_pars_fragment:lR,lights_fragment_begin:cR,lights_fragment_maps:uR,lights_fragment_end:dR,logdepthbuf_fragment:fR,logdepthbuf_pars_fragment:hR,logdepthbuf_pars_vertex:pR,logdepthbuf_vertex:mR,map_fragment:gR,map_pars_fragment:yR,map_particle_fragment:vR,map_particle_pars_fragment:_R,metalnessmap_fragment:xR,metalnessmap_pars_fragment:MR,morphinstance_vertex:ER,morphcolor_vertex:bR,morphnormal_vertex:wR,morphtarget_pars_vertex:SR,morphtarget_vertex:CR,normal_fragment_begin:TR,normal_fragment_maps:DR,normal_pars_fragment:AR,normal_pars_vertex:IR,normal_vertex:RR,normalmap_pars_fragment:NR,clearcoat_normal_fragment_begin:PR,clearcoat_normal_fragment_maps:OR,clearcoat_pars_fragment:LR,iridescence_pars_fragment:FR,opaque_fragment:kR,packing:UR,premultiplied_alpha_fragment:VR,project_vertex:BR,dithering_fragment:HR,dithering_pars_fragment:zR,roughnessmap_fragment:GR,roughnessmap_pars_fragment:WR,shadowmap_pars_fragment:jR,shadowmap_pars_vertex:$R,shadowmap_vertex:qR,shadowmask_pars_fragment:XR,skinbase_vertex:YR,skinning_pars_vertex:ZR,skinning_vertex:KR,skinnormal_vertex:JR,specularmap_fragment:QR,specularmap_pars_fragment:eN,tonemapping_fragment:tN,tonemapping_pars_fragment:nN,transmission_fragment:iN,transmission_pars_fragment:rN,uv_pars_fragment:sN,uv_pars_vertex:oN,uv_vertex:aN,worldpos_vertex:lN,background_vert:cN,background_frag:uN,backgroundCube_vert:dN,backgroundCube_frag:fN,cube_vert:hN,cube_frag:pN,depth_vert:mN,depth_frag:gN,distanceRGBA_vert:yN,distanceRGBA_frag:vN,equirect_vert:_N,equirect_frag:xN,linedashed_vert:MN,linedashed_frag:EN,meshbasic_vert:bN,meshbasic_frag:wN,meshlambert_vert:SN,meshlambert_frag:CN,meshmatcap_vert:TN,meshmatcap_frag:DN,meshnormal_vert:AN,meshnormal_frag:IN,meshphong_vert:RN,meshphong_frag:NN,meshphysical_vert:PN,meshphysical_frag:ON,meshtoon_vert:LN,meshtoon_frag:FN,points_vert:kN,points_frag:UN,shadow_vert:VN,shadow_frag:BN,sprite_vert:HN,sprite_frag:zN},se={common:{diffuse:{value:new xe(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Fe},alphaMap:{value:null},alphaMapTransform:{value:new Fe},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Fe}},envmap:{envMap:{value:null},envMapRotation:{value:new Fe},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Fe}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Fe}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Fe},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Fe},normalScale:{value:new te(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Fe},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Fe}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Fe}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Fe}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new xe(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMap:{value:[]},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotShadowMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMap:{value:[]},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new xe(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Fe},alphaTest:{value:0},uvTransform:{value:new Fe}},sprite:{diffuse:{value:new xe(16777215)},opacity:{value:1},center:{value:new te(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Fe},alphaMap:{value:null},alphaMapTransform:{value:new Fe},alphaTest:{value:0}}},ui={basic:{uniforms:nn([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.fog]),vertexShader:Ue.meshbasic_vert,fragmentShader:Ue.meshbasic_frag},lambert:{uniforms:nn([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.fog,se.lights,{emissive:{value:new xe(0)}}]),vertexShader:Ue.meshlambert_vert,fragmentShader:Ue.meshlambert_frag},phong:{uniforms:nn([se.common,se.specularmap,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.fog,se.lights,{emissive:{value:new xe(0)},specular:{value:new xe(1118481)},shininess:{value:30}}]),vertexShader:Ue.meshphong_vert,fragmentShader:Ue.meshphong_frag},standard:{uniforms:nn([se.common,se.envmap,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.roughnessmap,se.metalnessmap,se.fog,se.lights,{emissive:{value:new xe(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:Ue.meshphysical_vert,fragmentShader:Ue.meshphysical_frag},toon:{uniforms:nn([se.common,se.aomap,se.lightmap,se.emissivemap,se.bumpmap,se.normalmap,se.displacementmap,se.gradientmap,se.fog,se.lights,{emissive:{value:new xe(0)}}]),vertexShader:Ue.meshtoon_vert,fragmentShader:Ue.meshtoon_frag},matcap:{uniforms:nn([se.common,se.bumpmap,se.normalmap,se.displacementmap,se.fog,{matcap:{value:null}}]),vertexShader:Ue.meshmatcap_vert,fragmentShader:Ue.meshmatcap_frag},points:{uniforms:nn([se.points,se.fog]),vertexShader:Ue.points_vert,fragmentShader:Ue.points_frag},dashed:{uniforms:nn([se.common,se.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:Ue.linedashed_vert,fragmentShader:Ue.linedashed_frag},depth:{uniforms:nn([se.common,se.displacementmap]),vertexShader:Ue.depth_vert,fragmentShader:Ue.depth_frag},normal:{uniforms:nn([se.common,se.bumpmap,se.normalmap,se.displacementmap,{opacity:{value:1}}]),vertexShader:Ue.meshnormal_vert,fragmentShader:Ue.meshnormal_frag},sprite:{uniforms:nn([se.sprite,se.fog]),vertexShader:Ue.sprite_vert,fragmentShader:Ue.sprite_frag},background:{uniforms:{uvTransform:{value:new Fe},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:Ue.background_vert,fragmentShader:Ue.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Fe}},vertexShader:Ue.backgroundCube_vert,fragmentShader:Ue.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:Ue.cube_vert,fragmentShader:Ue.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:Ue.equirect_vert,fragmentShader:Ue.equirect_frag},distanceRGBA:{uniforms:nn([se.common,se.displacementmap,{referencePosition:{value:new R},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:Ue.distanceRGBA_vert,fragmentShader:Ue.distanceRGBA_frag},shadow:{uniforms:nn([se.lights,se.fog,{color:{value:new xe(0)},opacity:{value:1}}]),vertexShader:Ue.shadow_vert,fragmentShader:Ue.shadow_frag}};ui.physical={uniforms:nn([ui.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Fe},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Fe},clearcoatNormalScale:{value:new te(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Fe},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Fe},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Fe},sheen:{value:0},sheenColor:{value:new xe(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Fe},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Fe},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Fe},transmissionSamplerSize:{value:new te},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Fe},attenuationDistance:{value:0},attenuationColor:{value:new xe(0)},specularColor:{value:new xe(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Fe},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Fe},anisotropyVector:{value:new te},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Fe}}]),vertexShader:Ue.meshphysical_vert,fragmentShader:Ue.meshphysical_frag};var au={r:0,b:0,g:0},kr=new $r,GN=new tt;function WN(n,e,t,i,r,s,o){let a=new xe(0),l=s===!0?0:1,c,u,d=null,h=0,f=null;function m(M){let _=M.isScene===!0?M.background:null;return _&&_.isTexture&&(_=(M.backgroundBlurriness>0?t:e).get(_)),_}function v(M){let _=!1,y=m(M);y===null?p(a,l):y&&y.isColor&&(p(y,1),_=!0);let A=n.xr.getEnvironmentBlendMode();A==="additive"?i.buffers.color.setClear(0,0,0,1,o):A==="alpha-blend"&&i.buffers.color.setClear(0,0,0,0,o),(n.autoClear||_)&&(i.buffers.depth.setTest(!0),i.buffers.depth.setMask(!0),i.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function g(M,_){let y=m(_);y&&(y.isCubeTexture||y.mapping===ed)?(u===void 0&&(u=new wt(new qr(1,1,1),new Gt({name:"BackgroundCubeMaterial",uniforms:yo(ui.backgroundCube.uniforms),vertexShader:ui.backgroundCube.vertexShader,fragmentShader:ui.backgroundCube.fragmentShader,side:Yt,depthTest:!1,depthWrite:!1,fog:!1})),u.geometry.deleteAttribute("normal"),u.geometry.deleteAttribute("uv"),u.onBeforeRender=function(A,C,S){this.matrixWorld.copyPosition(S.matrixWorld)},Object.defineProperty(u.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),r.update(u)),kr.copy(_.backgroundRotation),kr.x*=-1,kr.y*=-1,kr.z*=-1,y.isCubeTexture&&y.isRenderTargetTexture===!1&&(kr.y*=-1,kr.z*=-1),u.material.uniforms.envMap.value=y,u.material.uniforms.flipEnvMap.value=y.isCubeTexture&&y.isRenderTargetTexture===!1?-1:1,u.material.uniforms.backgroundBlurriness.value=_.backgroundBlurriness,u.material.uniforms.backgroundIntensity.value=_.backgroundIntensity,u.material.uniforms.backgroundRotation.value.setFromMatrix4(GN.makeRotationFromEuler(kr)),u.material.toneMapped=Xe.getTransfer(y.colorSpace)!==ot,(d!==y||h!==y.version||f!==n.toneMapping)&&(u.material.needsUpdate=!0,d=y,h=y.version,f=n.toneMapping),u.layers.enableAll(),M.unshift(u,u.geometry,u.material,0,0,null)):y&&y.isTexture&&(c===void 0&&(c=new wt(new Xr(2,2),new Gt({name:"BackgroundMaterial",uniforms:yo(ui.background.uniforms),vertexShader:ui.background.vertexShader,fragmentShader:ui.background.fragmentShader,side:cr,depthTest:!1,depthWrite:!1,fog:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),r.update(c)),c.material.uniforms.t2D.value=y,c.material.uniforms.backgroundIntensity.value=_.backgroundIntensity,c.material.toneMapped=Xe.getTransfer(y.colorSpace)!==ot,y.matrixAutoUpdate===!0&&y.updateMatrix(),c.material.uniforms.uvTransform.value.copy(y.matrix),(d!==y||h!==y.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,d=y,h=y.version,f=n.toneMapping),c.layers.enableAll(),M.unshift(c,c.geometry,c.material,0,0,null))}function p(M,_){M.getRGB(au,eE(n)),i.buffers.color.setClear(au.r,au.g,au.b,_,o)}return{getClearColor:function(){return a},setClearColor:function(M,_=1){a.set(M),l=_,p(a,l)},getClearAlpha:function(){return l},setClearAlpha:function(M){l=M,p(a,l)},render:v,addToRenderList:g}}function jN(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=h(null),s=r,o=!1;function a(x,D,N,O,U){let $=!1,W=d(O,N,D);s!==W&&(s=W,c(s.object)),$=f(x,O,N,U),$&&m(x,O,N,U),U!==null&&e.update(U,n.ELEMENT_ARRAY_BUFFER),($||o)&&(o=!1,y(x,D,N,O),U!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(U).buffer))}function l(){return n.createVertexArray()}function c(x){return n.bindVertexArray(x)}function u(x){return n.deleteVertexArray(x)}function d(x,D,N){let O=N.wireframe===!0,U=i[x.id];U===void 0&&(U={},i[x.id]=U);let $=U[D.id];$===void 0&&($={},U[D.id]=$);let W=$[O];return W===void 0&&(W=h(l()),$[O]=W),W}function h(x){let D=[],N=[],O=[];for(let U=0;U<t;U++)D[U]=0,N[U]=0,O[U]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:D,enabledAttributes:N,attributeDivisors:O,object:x,attributes:{},index:null}}function f(x,D,N,O){let U=s.attributes,$=D.attributes,W=0,q=N.getAttributes();for(let z in q)if(q[z].location>=0){let ee=U[z],oe=$[z];if(oe===void 0&&(z==="instanceMatrix"&&x.instanceMatrix&&(oe=x.instanceMatrix),z==="instanceColor"&&x.instanceColor&&(oe=x.instanceColor)),ee===void 0||ee.attribute!==oe||oe&&ee.data!==oe.data)return!0;W++}return s.attributesNum!==W||s.index!==O}function m(x,D,N,O){let U={},$=D.attributes,W=0,q=N.getAttributes();for(let z in q)if(q[z].location>=0){let ee=$[z];ee===void 0&&(z==="instanceMatrix"&&x.instanceMatrix&&(ee=x.instanceMatrix),z==="instanceColor"&&x.instanceColor&&(ee=x.instanceColor));let oe={};oe.attribute=ee,ee&&ee.data&&(oe.data=ee.data),U[z]=oe,W++}s.attributes=U,s.attributesNum=W,s.index=O}function v(){let x=s.newAttributes;for(let D=0,N=x.length;D<N;D++)x[D]=0}function g(x){p(x,0)}function p(x,D){let N=s.newAttributes,O=s.enabledAttributes,U=s.attributeDivisors;N[x]=1,O[x]===0&&(n.enableVertexAttribArray(x),O[x]=1),U[x]!==D&&(n.vertexAttribDivisor(x,D),U[x]=D)}function M(){let x=s.newAttributes,D=s.enabledAttributes;for(let N=0,O=D.length;N<O;N++)D[N]!==x[N]&&(n.disableVertexAttribArray(N),D[N]=0)}function _(x,D,N,O,U,$,W){W===!0?n.vertexAttribIPointer(x,D,N,U,$):n.vertexAttribPointer(x,D,N,O,U,$)}function y(x,D,N,O){v();let U=O.attributes,$=N.getAttributes(),W=D.defaultAttributeValues;for(let q in $){let z=$[q];if(z.location>=0){let Q=U[q];if(Q===void 0&&(q==="instanceMatrix"&&x.instanceMatrix&&(Q=x.instanceMatrix),q==="instanceColor"&&x.instanceColor&&(Q=x.instanceColor)),Q!==void 0){let ee=Q.normalized,oe=Q.itemSize,ge=e.get(Q);if(ge===void 0)continue;let we=ge.buffer,G=ge.type,J=ge.bytesPerElement,fe=G===n.INT||G===n.UNSIGNED_INT||Q.gpuType===Tg;if(Q.isInterleavedBufferAttribute){let re=Q.data,be=re.stride,Te=Q.offset;if(re.isInstancedInterleavedBuffer){for(let Ne=0;Ne<z.locationSize;Ne++)p(z.location+Ne,re.meshPerAttribute);x.isInstancedMesh!==!0&&O._maxInstanceCount===void 0&&(O._maxInstanceCount=re.meshPerAttribute*re.count)}else for(let Ne=0;Ne<z.locationSize;Ne++)g(z.location+Ne);n.bindBuffer(n.ARRAY_BUFFER,we);for(let Ne=0;Ne<z.locationSize;Ne++)_(z.location+Ne,oe/z.locationSize,G,ee,be*J,(Te+oe/z.locationSize*Ne)*J,fe)}else{if(Q.isInstancedBufferAttribute){for(let re=0;re<z.locationSize;re++)p(z.location+re,Q.meshPerAttribute);x.isInstancedMesh!==!0&&O._maxInstanceCount===void 0&&(O._maxInstanceCount=Q.meshPerAttribute*Q.count)}else for(let re=0;re<z.locationSize;re++)g(z.location+re);n.bindBuffer(n.ARRAY_BUFFER,we);for(let re=0;re<z.locationSize;re++)_(z.location+re,oe/z.locationSize,G,ee,oe*J,oe/z.locationSize*re*J,fe)}}else if(W!==void 0){let ee=W[q];if(ee!==void 0)switch(ee.length){case 2:n.vertexAttrib2fv(z.location,ee);break;case 3:n.vertexAttrib3fv(z.location,ee);break;case 4:n.vertexAttrib4fv(z.location,ee);break;default:n.vertexAttrib1fv(z.location,ee)}}}}M()}function A(){I();for(let x in i){let D=i[x];for(let N in D){let O=D[N];for(let U in O)u(O[U].object),delete O[U];delete D[N]}delete i[x]}}function C(x){if(i[x.id]===void 0)return;let D=i[x.id];for(let N in D){let O=D[N];for(let U in O)u(O[U].object),delete O[U];delete D[N]}delete i[x.id]}function S(x){for(let D in i){let N=i[D];if(N[x.id]===void 0)continue;let O=N[x.id];for(let U in O)u(O[U].object),delete O[U];delete N[x.id]}}function I(){b(),o=!0,s!==r&&(s=r,c(s.object))}function b(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:I,resetDefaultState:b,dispose:A,releaseStatesOfGeometry:C,releaseStatesOfProgram:S,initAttributes:v,enableAttribute:g,disableUnusedAttributes:M}}function $N(n,e,t){let i;function r(c){i=c}function s(c,u){n.drawArrays(i,c,u),t.update(u,i,1)}function o(c,u,d){d!==0&&(n.drawArraysInstanced(i,c,u,d),t.update(u,i,d))}function a(c,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,u,0,d);let f=0;for(let m=0;m<d;m++)f+=u[m];t.update(f,i,1)}function l(c,u,d,h){if(d===0)return;let f=e.get("WEBGL_multi_draw");if(f===null)for(let m=0;m<c.length;m++)o(c[m],u[m],h[m]);else{f.multiDrawArraysInstancedWEBGL(i,c,0,u,0,h,0,d);let m=0;for(let v=0;v<d;v++)m+=u[v]*h[v];t.update(m,i,1)}}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a,this.renderMultiDrawInstances=l}function qN(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let S=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(S.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(S){return!(S!==$n&&i.convert(S)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(S){let I=S===ns&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(S!==Ui&&i.convert(S)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&S!==di&&!I)}function l(S){if(S==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";S="mediump"}return S==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let c=t.precision!==void 0?t.precision:"highp",u=l(c);u!==c&&(console.warn("THREE.WebGLRenderer:",c,"not supported, using",u,"instead."),c=u);let d=t.logarithmicDepthBuffer===!0,h=t.reverseDepthBuffer===!0&&e.has("EXT_clip_control"),f=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),m=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),v=n.getParameter(n.MAX_TEXTURE_SIZE),g=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),M=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),_=n.getParameter(n.MAX_VARYING_VECTORS),y=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),A=m>0,C=n.getParameter(n.MAX_SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:l,textureFormatReadable:o,textureTypeReadable:a,precision:c,logarithmicDepthBuffer:d,reverseDepthBuffer:h,maxTextures:f,maxVertexTextures:m,maxTextureSize:v,maxCubemapSize:g,maxAttributes:p,maxVertexUniforms:M,maxVaryings:_,maxFragmentUniforms:y,vertexTextures:A,maxSamples:C}}function XN(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Wn,a=new Fe,l={value:null,needsUpdate:!1};this.uniform=l,this.numPlanes=0,this.numIntersection=0,this.init=function(d,h){let f=d.length!==0||h||i!==0||r;return r=h,i=d.length,f},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,h){t=u(d,h,0)},this.setState=function(d,h,f){let m=d.clippingPlanes,v=d.clipIntersection,g=d.clipShadows,p=n.get(d);if(!r||m===null||m.length===0||s&&!g)s?u(null):c();else{let M=s?0:i,_=M*4,y=p.clippingState||null;l.value=y,y=u(m,h,_,f);for(let A=0;A!==_;++A)y[A]=t[A];p.clippingState=y,this.numIntersection=v?this.numPlanes:0,this.numPlanes+=M}};function c(){l.value!==t&&(l.value=t,l.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,h,f,m){let v=d!==null?d.length:0,g=null;if(v!==0){if(g=l.value,m!==!0||g===null){let p=f+v*4,M=h.matrixWorldInverse;a.getNormalMatrix(M),(g===null||g.length<p)&&(g=new Float32Array(p));for(let _=0,y=f;_!==v;++_,y+=4)o.copy(d[_]).applyMatrix4(M,a),o.normal.toArray(g,y),g[y+3]=o.constant}l.value=g,l.needsUpdate=!0}return e.numPlanes=v,e.numIntersection=0,g}}function YN(n){let e=new WeakMap;function t(o,a){return a===om?o.mapping=fo:a===am&&(o.mapping=ho),o}function i(o){if(o&&o.isTexture){let a=o.mapping;if(a===om||a===am)if(e.has(o)){let l=e.get(o).texture;return t(l,o.mapping)}else{let l=o.image;if(l&&l.height>0){let c=new zm(l.height);return c.fromEquirectangularTexture(n,o),e.set(o,c),o.addEventListener("dispose",r),t(c.texture,o.mapping)}else return null}}return o}function r(o){let a=o.target;a.removeEventListener("dispose",r);let l=e.get(a);l!==void 0&&(e.delete(a),l.dispose())}function s(){e=new WeakMap}return{get:i,dispose:s}}var vo=class extends Ru{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,l=r-t;if(this.view!==null&&this.view.enabled){let c=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=c*this.view.offsetX,o=s+c*this.view.width,a-=u*this.view.offsetY,l=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,l,this.near,this.far,this.coordinateSystem),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},ro=4,iM=[.125,.215,.35,.446,.526,.582],Hr=20,Bp=new vo,rM=new xe,Hp=null,zp=0,Gp=0,Wp=!1,Vr=(1+Math.sqrt(5))/2,Js=1/Vr,sM=[new R(-Vr,Js,0),new R(Vr,Js,0),new R(-Js,0,Vr),new R(Js,0,Vr),new R(0,Vr,-Js),new R(0,Vr,Js),new R(-1,1,-1),new R(1,1,-1),new R(-1,1,1),new R(1,1,1)],Pu=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._lodPlanes=[],this._sizeLods=[],this._sigmas=[],this._blurMaterial=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._compileMaterial(this._blurMaterial)}fromScene(e,t=0,i=.1,r=100){Hp=this._renderer.getRenderTarget(),zp=this._renderer.getActiveCubeFace(),Gp=this._renderer.getActiveMipmapLevel(),Wp=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(256);let s=this._allocateTargets();return s.depthBuffer=!0,this._sceneToCubeUV(e,i,r,s),t>0&&this._blur(s,0,0,t),this._applyPMREM(s),this._cleanup(s),s}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=lM(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=aM(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose()}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodPlanes.length;e++)this._lodPlanes[e].dispose()}_cleanup(e){this._renderer.setRenderTarget(Hp,zp,Gp),this._renderer.xr.enabled=Wp,e.scissorTest=!1,lu(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===fo||e.mapping===ho?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),Hp=this._renderer.getRenderTarget(),zp=this._renderer.getActiveCubeFace(),Gp=this._renderer.getActiveMipmapLevel(),Wp=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:vn,minFilter:vn,generateMipmaps:!1,type:ns,format:$n,colorSpace:So,depthBuffer:!1},r=oM(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=oM(e,t,i);let{_lodMax:s}=this;({sizeLods:this._sizeLods,lodPlanes:this._lodPlanes,sigmas:this._sigmas}=ZN(s)),this._blurMaterial=KN(s,e,t)}return r}_compileMaterial(e){let t=new wt(this._lodPlanes[0],e);this._renderer.compile(t,Bp)}_sceneToCubeUV(e,t,i,r){let a=new sn(90,1,t,i),l=[1,-1,1,1,1,1],c=[1,1,1,-1,-1,-1],u=this._renderer,d=u.autoClear,h=u.toneMapping;u.getClearColor(rM),u.toneMapping=pi,u.autoClear=!1;let f=new go({name:"PMREM.Background",side:Yt,depthWrite:!1,depthTest:!1}),m=new wt(new qr,f),v=!1,g=e.background;g?g.isColor&&(f.color.copy(g),e.background=null,v=!0):(f.color.copy(rM),v=!0);for(let p=0;p<6;p++){let M=p%3;M===0?(a.up.set(0,l[p],0),a.lookAt(c[p],0,0)):M===1?(a.up.set(0,0,l[p]),a.lookAt(0,c[p],0)):(a.up.set(0,l[p],0),a.lookAt(0,0,c[p]));let _=this._cubeSize;lu(r,M*_,p>2?_:0,_,_),u.setRenderTarget(r),v&&u.render(m,a),u.render(e,a)}m.geometry.dispose(),m.material.dispose(),u.toneMapping=h,u.autoClear=d,e.background=g}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===fo||e.mapping===ho;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=lM()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=aM());let s=r?this._cubemapMaterial:this._equirectMaterial,o=new wt(this._lodPlanes[0],s),a=s.uniforms;a.envMap.value=e;let l=this._cubeSize;lu(t,0,0,3*l,2*l),i.setRenderTarget(t),i.render(o,Bp)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodPlanes.length;for(let s=1;s<r;s++){let o=Math.sqrt(this._sigmas[s]*this._sigmas[s]-this._sigmas[s-1]*this._sigmas[s-1]),a=sM[(r-s-1)%sM.length];this._blur(e,s-1,s,o,a)}t.autoClear=i}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let l=this._renderer,c=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&console.error("blur direction must be either latitudinal or longitudinal!");let u=3,d=new wt(this._lodPlanes[r],c),h=c.uniforms,f=this._sizeLods[i]-1,m=isFinite(s)?Math.PI/(2*f):2*Math.PI/(2*Hr-1),v=s/m,g=isFinite(s)?1+Math.floor(u*v):Hr;g>Hr&&console.warn(`sigmaRadians, ${s}, is too large and will clip, as it requested ${g} samples when the maximum is set to ${Hr}`);let p=[],M=0;for(let S=0;S<Hr;++S){let I=S/v,b=Math.exp(-I*I/2);p.push(b),S===0?M+=b:S<g&&(M+=2*b)}for(let S=0;S<p.length;S++)p[S]=p[S]/M;h.envMap.value=e.texture,h.samples.value=g,h.weights.value=p,h.latitudinal.value=o==="latitudinal",a&&(h.poleAxis.value=a);let{_lodMax:_}=this;h.dTheta.value=m,h.mipInt.value=_-i;let y=this._sizeLods[r],A=3*y*(r>_-ro?r-_+ro:0),C=4*(this._cubeSize-y);lu(t,A,C,3*y,2*y),l.setRenderTarget(t),l.render(d,Bp)}};function ZN(n){let e=[],t=[],i=[],r=n,s=n-ro+1+iM.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);t.push(a);let l=1/a;o>n-ro?l=iM[o-n+ro-1]:o===0&&(l=0),i.push(l);let c=1/(a-2),u=-c,d=1+c,h=[u,u,d,u,d,d,u,u,d,d,u,d],f=6,m=6,v=3,g=2,p=1,M=new Float32Array(v*m*f),_=new Float32Array(g*m*f),y=new Float32Array(p*m*f);for(let C=0;C<f;C++){let S=C%3*2/3-1,I=C>2?0:-1,b=[S,I,0,S+2/3,I,0,S+2/3,I+1,0,S,I,0,S+2/3,I+1,0,S,I+1,0];M.set(b,v*m*C),_.set(h,g*m*C);let x=[C,C,C,C,C,C];y.set(x,p*m*C)}let A=new zt;A.setAttribute("position",new Zt(M,v)),A.setAttribute("uv",new Zt(_,g)),A.setAttribute("faceIndex",new Zt(y,p)),e.push(A),r>ro&&r--}return{lodPlanes:e,sizeLods:t,sigmas:i}}function oM(n,e,t){let i=new xn(n,e,t);return i.texture.mapping=ed,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function lu(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function KN(n,e,t){let i=new Float32Array(Hr),r=new R(0,1,0);return new Gt({name:"SphericalGaussianBlur",defines:{n:Hr,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:Fg(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:hi,depthTest:!1,depthWrite:!1})}function aM(){return new Gt({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:Fg(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:hi,depthTest:!1,depthWrite:!1})}function lM(){return new Gt({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:Fg(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:hi,depthTest:!1,depthWrite:!1})}function Fg(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}function JN(n){let e=new WeakMap,t=null;function i(a){if(a&&a.isTexture){let l=a.mapping,c=l===om||l===am,u=l===fo||l===ho;if(c||u){let d=e.get(a),h=d!==void 0?d.texture.pmremVersion:0;if(a.isRenderTargetTexture&&a.pmremVersion!==h)return t===null&&(t=new Pu(n)),d=c?t.fromEquirectangular(a,d):t.fromCubemap(a,d),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),d.texture;if(d!==void 0)return d.texture;{let f=a.image;return c&&f&&f.height>0||u&&f&&r(f)?(t===null&&(t=new Pu(n)),d=c?t.fromEquirectangular(a):t.fromCubemap(a),d.texture.pmremVersion=a.pmremVersion,e.set(a,d),a.addEventListener("dispose",s),d.texture):null}}}return a}function r(a){let l=0,c=6;for(let u=0;u<c;u++)a[u]!==void 0&&l++;return l===c}function s(a){let l=a.target;l.removeEventListener("dispose",s);let c=e.get(l);c!==void 0&&(e.delete(l),c.dispose())}function o(){e=new WeakMap,t!==null&&(t.dispose(),t=null)}return{get:i,dispose:o}}function QN(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r;switch(i){case"WEBGL_depth_texture":r=n.getExtension("WEBGL_depth_texture")||n.getExtension("MOZ_WEBGL_depth_texture")||n.getExtension("WEBKIT_WEBGL_depth_texture");break;case"EXT_texture_filter_anisotropic":r=n.getExtension("EXT_texture_filter_anisotropic")||n.getExtension("MOZ_EXT_texture_filter_anisotropic")||n.getExtension("WEBKIT_EXT_texture_filter_anisotropic");break;case"WEBGL_compressed_texture_s3tc":r=n.getExtension("WEBGL_compressed_texture_s3tc")||n.getExtension("MOZ_WEBGL_compressed_texture_s3tc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_s3tc");break;case"WEBGL_compressed_texture_pvrtc":r=n.getExtension("WEBGL_compressed_texture_pvrtc")||n.getExtension("WEBKIT_WEBGL_compressed_texture_pvrtc");break;default:r=n.getExtension(i)}return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Aa("THREE.WebGLRenderer: "+i+" extension not supported."),r}}}function eP(n,e,t,i){let r={},s=new WeakMap;function o(d){let h=d.target;h.index!==null&&e.remove(h.index);for(let m in h.attributes)e.remove(h.attributes[m]);for(let m in h.morphAttributes){let v=h.morphAttributes[m];for(let g=0,p=v.length;g<p;g++)e.remove(v[g])}h.removeEventListener("dispose",o),delete r[h.id];let f=s.get(h);f&&(e.remove(f),s.delete(h)),i.releaseStatesOfGeometry(h),h.isInstancedBufferGeometry===!0&&delete h._maxInstanceCount,t.memory.geometries--}function a(d,h){return r[h.id]===!0||(h.addEventListener("dispose",o),r[h.id]=!0,t.memory.geometries++),h}function l(d){let h=d.attributes;for(let m in h)e.update(h[m],n.ARRAY_BUFFER);let f=d.morphAttributes;for(let m in f){let v=f[m];for(let g=0,p=v.length;g<p;g++)e.update(v[g],n.ARRAY_BUFFER)}}function c(d){let h=[],f=d.index,m=d.attributes.position,v=0;if(f!==null){let M=f.array;v=f.version;for(let _=0,y=M.length;_<y;_+=3){let A=M[_+0],C=M[_+1],S=M[_+2];h.push(A,C,C,S,S,A)}}else if(m!==void 0){let M=m.array;v=m.version;for(let _=0,y=M.length/3-1;_<y;_+=3){let A=_+0,C=_+1,S=_+2;h.push(A,C,C,S,S,A)}}else return;let g=new(JM(h)?Iu:Au)(h,1);g.version=v;let p=s.get(d);p&&e.remove(p),s.set(d,g)}function u(d){let h=s.get(d);if(h){let f=d.index;f!==null&&h.version<f.version&&c(d)}else c(d);return s.get(d)}return{get:a,update:l,getWireframeAttribute:u}}function tP(n,e,t){let i;function r(h){i=h}let s,o;function a(h){s=h.type,o=h.bytesPerElement}function l(h,f){n.drawElements(i,f,s,h*o),t.update(f,i,1)}function c(h,f,m){m!==0&&(n.drawElementsInstanced(i,f,s,h*o,m),t.update(f,i,m))}function u(h,f,m){if(m===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,f,0,s,h,0,m);let g=0;for(let p=0;p<m;p++)g+=f[p];t.update(g,i,1)}function d(h,f,m,v){if(m===0)return;let g=e.get("WEBGL_multi_draw");if(g===null)for(let p=0;p<h.length;p++)c(h[p]/o,f[p],v[p]);else{g.multiDrawElementsInstancedWEBGL(i,f,0,s,h,0,v,0,m);let p=0;for(let M=0;M<m;M++)p+=f[M]*v[M];t.update(p,i,1)}}this.setMode=r,this.setIndex=a,this.render=l,this.renderInstances=c,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function nP(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:console.error("THREE.WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function iP(n,e,t){let i=new WeakMap,r=new At;function s(o,a,l){let c=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,h=i.get(a);if(h===void 0||h.count!==d){let x=function(){I.dispose(),i.delete(a),a.removeEventListener("dispose",x)};var f=x;h!==void 0&&h.texture.dispose();let m=a.morphAttributes.position!==void 0,v=a.morphAttributes.normal!==void 0,g=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],M=a.morphAttributes.normal||[],_=a.morphAttributes.color||[],y=0;m===!0&&(y=1),v===!0&&(y=2),g===!0&&(y=3);let A=a.attributes.position.count*y,C=1;A>e.maxTextureSize&&(C=Math.ceil(A/e.maxTextureSize),A=e.maxTextureSize);let S=new Float32Array(A*C*4*d),I=new Tu(S,A,C,d);I.type=di,I.needsUpdate=!0;let b=y*4;for(let D=0;D<d;D++){let N=p[D],O=M[D],U=_[D],$=A*C*4*D;for(let W=0;W<N.count;W++){let q=W*b;m===!0&&(r.fromBufferAttribute(N,W),S[$+q+0]=r.x,S[$+q+1]=r.y,S[$+q+2]=r.z,S[$+q+3]=0),v===!0&&(r.fromBufferAttribute(O,W),S[$+q+4]=r.x,S[$+q+5]=r.y,S[$+q+6]=r.z,S[$+q+7]=0),g===!0&&(r.fromBufferAttribute(U,W),S[$+q+8]=r.x,S[$+q+9]=r.y,S[$+q+10]=r.z,S[$+q+11]=U.itemSize===4?r.w:1)}}h={count:d,texture:I,size:new te(A,C)},i.set(a,h),a.addEventListener("dispose",x)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)l.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let m=0;for(let g=0;g<c.length;g++)m+=c[g];let v=a.morphTargetsRelative?1:1-m;l.getUniforms().setValue(n,"morphTargetBaseInfluence",v),l.getUniforms().setValue(n,"morphTargetInfluences",c)}l.getUniforms().setValue(n,"morphTargetsTexture",h.texture,t),l.getUniforms().setValue(n,"morphTargetsTextureSize",h.size)}return{update:s}}function rP(n,e,t,i){let r=new WeakMap;function s(l){let c=i.render.frame,u=l.geometry,d=e.get(l,u);if(r.get(d)!==c&&(e.update(d),r.set(d,c)),l.isInstancedMesh&&(l.hasEventListener("dispose",a)===!1&&l.addEventListener("dispose",a),r.get(l)!==c&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),r.set(l,c))),l.isSkinnedMesh){let h=l.skeleton;r.get(h)!==c&&(h.update(),r.set(h,c))}return d}function o(){r=new WeakMap}function a(l){let c=l.target;c.removeEventListener("dispose",a),t.remove(c.instanceMatrix),c.instanceColor!==null&&t.remove(c.instanceColor)}return{update:s,dispose:o}}var Yr=class extends zi{constructor(e,t,i,r,s,o,a,l,c,u=ao){if(u!==ao&&u!==mo)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");i===void 0&&u===ao&&(i=Wr),i===void 0&&u===mo&&(i=po),super(null,r,s,o,a,l,u,i,c),this.isDepthTexture=!0,this.image={width:e,height:t},this.magFilter=a!==void 0?a:_n,this.minFilter=l!==void 0?l:_n,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},nE=new zi,cM=new Yr(1,1),iE=new Tu,rE=new Bm,sE=new Nu,uM=[],dM=[],fM=new Float32Array(16),hM=new Float32Array(9),pM=new Float32Array(4);function Co(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=uM[r];if(s===void 0&&(s=new Float32Array(r),uM[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Ot(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Lt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function nd(n,e){let t=dM[e];t===void 0&&(t=new Int32Array(e),dM[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function sP(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function oP(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ot(t,e))return;n.uniform2fv(this.addr,e),Lt(t,e)}}function aP(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Ot(t,e))return;n.uniform3fv(this.addr,e),Lt(t,e)}}function lP(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ot(t,e))return;n.uniform4fv(this.addr,e),Lt(t,e)}}function cP(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ot(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Lt(t,e)}else{if(Ot(t,i))return;pM.set(i),n.uniformMatrix2fv(this.addr,!1,pM),Lt(t,i)}}function uP(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ot(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Lt(t,e)}else{if(Ot(t,i))return;hM.set(i),n.uniformMatrix3fv(this.addr,!1,hM),Lt(t,i)}}function dP(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Ot(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Lt(t,e)}else{if(Ot(t,i))return;fM.set(i),n.uniformMatrix4fv(this.addr,!1,fM),Lt(t,i)}}function fP(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function hP(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ot(t,e))return;n.uniform2iv(this.addr,e),Lt(t,e)}}function pP(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Ot(t,e))return;n.uniform3iv(this.addr,e),Lt(t,e)}}function mP(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ot(t,e))return;n.uniform4iv(this.addr,e),Lt(t,e)}}function gP(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function yP(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Ot(t,e))return;n.uniform2uiv(this.addr,e),Lt(t,e)}}function vP(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Ot(t,e))return;n.uniform3uiv(this.addr,e),Lt(t,e)}}function _P(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Ot(t,e))return;n.uniform4uiv(this.addr,e),Lt(t,e)}}function xP(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(cM.compareFunction=YM,s=cM):s=nE,t.setTexture2D(e||s,r)}function MP(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||rE,r)}function EP(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||sE,r)}function bP(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||iE,r)}function wP(n){switch(n){case 5126:return sP;case 35664:return oP;case 35665:return aP;case 35666:return lP;case 35674:return cP;case 35675:return uP;case 35676:return dP;case 5124:case 35670:return fP;case 35667:case 35671:return hP;case 35668:case 35672:return pP;case 35669:case 35673:return mP;case 5125:return gP;case 36294:return yP;case 36295:return vP;case 36296:return _P;case 35678:case 36198:case 36298:case 36306:case 35682:return xP;case 35679:case 36299:case 36307:return MP;case 35680:case 36300:case 36308:case 36293:return EP;case 36289:case 36303:case 36311:case 36292:return bP}}function SP(n,e){n.uniform1fv(this.addr,e)}function CP(n,e){let t=Co(e,this.size,2);n.uniform2fv(this.addr,t)}function TP(n,e){let t=Co(e,this.size,3);n.uniform3fv(this.addr,t)}function DP(n,e){let t=Co(e,this.size,4);n.uniform4fv(this.addr,t)}function AP(n,e){let t=Co(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function IP(n,e){let t=Co(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function RP(n,e){let t=Co(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function NP(n,e){n.uniform1iv(this.addr,e)}function PP(n,e){n.uniform2iv(this.addr,e)}function OP(n,e){n.uniform3iv(this.addr,e)}function LP(n,e){n.uniform4iv(this.addr,e)}function FP(n,e){n.uniform1uiv(this.addr,e)}function kP(n,e){n.uniform2uiv(this.addr,e)}function UP(n,e){n.uniform3uiv(this.addr,e)}function VP(n,e){n.uniform4uiv(this.addr,e)}function BP(n,e,t){let i=this.cache,r=e.length,s=nd(t,r);Ot(i,s)||(n.uniform1iv(this.addr,s),Lt(i,s));for(let o=0;o!==r;++o)t.setTexture2D(e[o]||nE,s[o])}function HP(n,e,t){let i=this.cache,r=e.length,s=nd(t,r);Ot(i,s)||(n.uniform1iv(this.addr,s),Lt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||rE,s[o])}function zP(n,e,t){let i=this.cache,r=e.length,s=nd(t,r);Ot(i,s)||(n.uniform1iv(this.addr,s),Lt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||sE,s[o])}function GP(n,e,t){let i=this.cache,r=e.length,s=nd(t,r);Ot(i,s)||(n.uniform1iv(this.addr,s),Lt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||iE,s[o])}function WP(n){switch(n){case 5126:return SP;case 35664:return CP;case 35665:return TP;case 35666:return DP;case 35674:return AP;case 35675:return IP;case 35676:return RP;case 5124:case 35670:return NP;case 35667:case 35671:return PP;case 35668:case 35672:return OP;case 35669:case 35673:return LP;case 5125:return FP;case 36294:return kP;case 36295:return UP;case 36296:return VP;case 35678:case 36198:case 36298:case 36306:case 35682:return BP;case 35679:case 36299:case 36307:return HP;case 35680:case 36300:case 36308:case 36293:return zP;case 36289:case 36303:case 36311:case 36292:return GP}}var Gm=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=wP(t.type)}},Wm=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=WP(t.type)}},jm=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},jp=/(\w+)(\])?(\[|\.)?/g;function mM(n,e){n.seq.push(e),n.map[e.id]=e}function jP(n,e,t){let i=n.name,r=i.length;for(jp.lastIndex=0;;){let s=jp.exec(i),o=jp.lastIndex,a=s[1],l=s[2]==="]",c=s[3];if(l&&(a=a|0),c===void 0||c==="["&&o+2===r){mM(t,c===void 0?new Gm(a,n,e):new Wm(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new jm(a),mM(t,d)),t=d}}}var co=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let r=0;r<i;++r){let s=e.getActiveUniform(t,r),o=e.getUniformLocation(t,s.name);jP(s,o,this)}}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],l=i[a.id];l.needsUpdate!==!1&&a.setValue(e,l.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function gM(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var $P=37297,qP=0;function XP(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var yM=new Fe;function YP(n){Xe._getMatrix(yM,Xe.workingColorSpace,n);let e=`mat3( ${yM.elements.map(t=>t.toFixed(4))} )`;switch(Xe.getTransfer(n)){case td:return[e,"LinearTransferOETF"];case ot:return[e,"sRGBTransferOETF"];default:return console.warn("THREE.WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function vM(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),r=n.getShaderInfoLog(e).trim();if(i&&r==="")return"";let s=/ERROR: 0:(\d+)/.exec(r);if(s){let o=parseInt(s[1]);return t.toUpperCase()+`

`+r+`

`+XP(n.getShaderSource(e),o)}else return r}function ZP(n,e){let t=YP(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}function KP(n,e){let t;switch(e){case Mg:t="Linear";break;case Eg:t="Reinhard";break;case bg:t="Cineon";break;case wg:t="ACESFilmic";break;case Sg:t="AgX";break;case Cg:t="Neutral";break;case pI:t="Custom";break;default:console.warn("THREE.WebGLProgram: Unsupported toneMapping:",e),t="Linear"}return"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var cu=new R;function JP(){Xe.getLuminanceCoefficients(cu);let n=cu.x.toFixed(4),e=cu.y.toFixed(4),t=cu.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function QP(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Ia).join(`
`)}function eO(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function tO(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function Ia(n){return n!==""}function _M(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function xM(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var nO=/^[ \t]*#include +<([\w\d./]+)>/gm;function $m(n){return n.replace(nO,rO)}var iO=new Map;function rO(n,e){let t=Ue[e];if(t===void 0){let i=iO.get(e);if(i!==void 0)t=Ue[i],console.warn('THREE.WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return $m(t)}var sO=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function MM(n){return n.replace(sO,oO)}function oO(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function EM(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}function aO(n){let e="SHADOWMAP_TYPE_BASIC";return n.shadowMapType===VM?e="SHADOWMAP_TYPE_PCF":n.shadowMapType===qA?e="SHADOWMAP_TYPE_PCF_SOFT":n.shadowMapType===Li&&(e="SHADOWMAP_TYPE_VSM"),e}function lO(n){let e="ENVMAP_TYPE_CUBE";if(n.envMap)switch(n.envMapMode){case fo:case ho:e="ENVMAP_TYPE_CUBE";break;case ed:e="ENVMAP_TYPE_CUBE_UV";break}return e}function cO(n){let e="ENVMAP_MODE_REFLECTION";return n.envMap&&n.envMapMode===ho&&(e="ENVMAP_MODE_REFRACTION"),e}function uO(n){let e="ENVMAP_BLENDING_NONE";if(n.envMap)switch(n.combine){case xg:e="ENVMAP_BLENDING_MULTIPLY";break;case fI:e="ENVMAP_BLENDING_MIX";break;case hI:e="ENVMAP_BLENDING_ADD";break}return e}function dO(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function fO(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,l=aO(t),c=lO(t),u=cO(t),d=uO(t),h=dO(t),f=QP(t),m=eO(s),v=r.createProgram(),g,p,M=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(g=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m].filter(Ia).join(`
`),g.length>0&&(g+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m].filter(Ia).join(`
`),p.length>0&&(p+=`
`)):(g=[EM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Ia).join(`
`),p=[EM(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+c:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",h?"#define CUBEUV_TEXEL_WIDTH "+h.texelWidth:"",h?"#define CUBEUV_TEXEL_HEIGHT "+h.texelHeight:"",h?"#define CUBEUV_MAX_MIP "+h.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor||t.batchingColor?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+l:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGDEPTHBUF":"",t.reverseDepthBuffer?"#define USE_REVERSEDEPTHBUF":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==pi?"#define TONE_MAPPING":"",t.toneMapping!==pi?Ue.tonemapping_pars_fragment:"",t.toneMapping!==pi?KP("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",Ue.colorspace_pars_fragment,ZP("linearToOutputTexel",t.outputColorSpace),JP(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Ia).join(`
`)),o=$m(o),o=_M(o,t),o=xM(o,t),a=$m(a),a=_M(a,t),a=xM(a,t),o=MM(o),a=MM(a),t.isRawShaderMaterial!==!0&&(M=`#version 300 es
`,g=[f,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+g,p=["#define varying in",t.glslVersion===Lx?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===Lx?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let _=M+g+o,y=M+p+a,A=gM(r,r.VERTEX_SHADER,_),C=gM(r,r.FRAGMENT_SHADER,y);r.attachShader(v,A),r.attachShader(v,C),t.index0AttributeName!==void 0?r.bindAttribLocation(v,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(v,0,"position"),r.linkProgram(v);function S(D){if(n.debug.checkShaderErrors){let N=r.getProgramInfoLog(v).trim(),O=r.getShaderInfoLog(A).trim(),U=r.getShaderInfoLog(C).trim(),$=!0,W=!0;if(r.getProgramParameter(v,r.LINK_STATUS)===!1)if($=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,v,A,C);else{let q=vM(r,A,"vertex"),z=vM(r,C,"fragment");console.error("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(v,r.VALIDATE_STATUS)+`

Material Name: `+D.name+`
Material Type: `+D.type+`

Program Info Log: `+N+`
`+q+`
`+z)}else N!==""?console.warn("THREE.WebGLProgram: Program Info Log:",N):(O===""||U==="")&&(W=!1);W&&(D.diagnostics={runnable:$,programLog:N,vertexShader:{log:O,prefix:g},fragmentShader:{log:U,prefix:p}})}r.deleteShader(A),r.deleteShader(C),I=new co(r,v),b=tO(r,v)}let I;this.getUniforms=function(){return I===void 0&&S(this),I};let b;this.getAttributes=function(){return b===void 0&&S(this),b};let x=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return x===!1&&(x=r.getProgramParameter(v,$P)),x},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(v),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=qP++,this.cacheKey=e,this.usedTimes=1,this.program=v,this.vertexShader=A,this.fragmentShader=C,this}var hO=0,qm=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Xm(e),t.set(e,i)),i}},Xm=class{constructor(e){this.id=hO++,this.code=e,this.usedTimes=0}};function pO(n,e,t,i,r,s,o){let a=new Du,l=new qm,c=new Set,u=[],d=r.logarithmicDepthBuffer,h=r.vertexTextures,f=r.precision,m={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distanceRGBA",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function v(b){return c.add(b),b===0?"uv":`uv${b}`}function g(b,x,D,N,O){let U=N.fog,$=O.geometry,W=b.isMeshStandardMaterial?N.environment:null,q=(b.isMeshStandardMaterial?t:e).get(b.envMap||W),z=q&&q.mapping===ed?q.image.height:null,Q=m[b.type];b.precision!==null&&(f=r.getMaxPrecision(b.precision),f!==b.precision&&console.warn("THREE.WebGLProgram.getParameters:",b.precision,"not supported, using",f,"instead."));let ee=$.morphAttributes.position||$.morphAttributes.normal||$.morphAttributes.color,oe=ee!==void 0?ee.length:0,ge=0;$.morphAttributes.position!==void 0&&(ge=1),$.morphAttributes.normal!==void 0&&(ge=2),$.morphAttributes.color!==void 0&&(ge=3);let we,G,J,fe;if(Q){let at=ui[Q];we=at.vertexShader,G=at.fragmentShader}else we=b.vertexShader,G=b.fragmentShader,l.update(b),J=l.getVertexShaderID(b),fe=l.getFragmentShaderID(b);let re=n.getRenderTarget(),be=n.state.buffers.depth.getReversed(),Te=O.isInstancedMesh===!0,Ne=O.isBatchedMesh===!0,nt=!!b.map,ze=!!b.matcap,_t=!!q,k=!!b.aoMap,wn=!!b.lightMap,Ye=!!b.bumpMap,Ze=!!b.normalMap,Ce=!!b.displacementMap,xt=!!b.emissiveMap,Se=!!b.metalnessMap,T=!!b.roughnessMap,E=b.anisotropy>0,V=b.clearcoat>0,Y=b.dispersion>0,K=b.iridescence>0,X=b.sheen>0,Me=b.transmission>0,le=E&&!!b.anisotropyMap,he=V&&!!b.clearcoatMap,Qe=V&&!!b.clearcoatNormalMap,ne=V&&!!b.clearcoatRoughnessMap,pe=K&&!!b.iridescenceMap,De=K&&!!b.iridescenceThicknessMap,Ae=X&&!!b.sheenColorMap,me=X&&!!b.sheenRoughnessMap,Ke=!!b.specularMap,ke=!!b.specularColorMap,gt=!!b.specularIntensityMap,P=Me&&!!b.transmissionMap,ae=Me&&!!b.thicknessMap,j=!!b.gradientMap,Z=!!b.alphaMap,de=b.alphaTest>0,ce=!!b.alphaHash,Oe=!!b.extensions,Tt=pi;b.toneMapped&&(re===null||re.isXRRenderTarget===!0)&&(Tt=n.toneMapping);let Wt={shaderID:Q,shaderType:b.type,shaderName:b.name,vertexShader:we,fragmentShader:G,defines:b.defines,customVertexShaderID:J,customFragmentShaderID:fe,isRawShaderMaterial:b.isRawShaderMaterial===!0,glslVersion:b.glslVersion,precision:f,batching:Ne,batchingColor:Ne&&O._colorsTexture!==null,instancing:Te,instancingColor:Te&&O.instanceColor!==null,instancingMorph:Te&&O.morphTexture!==null,supportsVertexTextures:h,outputColorSpace:re===null?n.outputColorSpace:re.isXRRenderTarget===!0?re.texture.colorSpace:So,alphaToCoverage:!!b.alphaToCoverage,map:nt,matcap:ze,envMap:_t,envMapMode:_t&&q.mapping,envMapCubeUVHeight:z,aoMap:k,lightMap:wn,bumpMap:Ye,normalMap:Ze,displacementMap:h&&Ce,emissiveMap:xt,normalMapObjectSpace:Ze&&b.normalMapType===vI,normalMapTangentSpace:Ze&&b.normalMapType===Og,metalnessMap:Se,roughnessMap:T,anisotropy:E,anisotropyMap:le,clearcoat:V,clearcoatMap:he,clearcoatNormalMap:Qe,clearcoatRoughnessMap:ne,dispersion:Y,iridescence:K,iridescenceMap:pe,iridescenceThicknessMap:De,sheen:X,sheenColorMap:Ae,sheenRoughnessMap:me,specularMap:Ke,specularColorMap:ke,specularIntensityMap:gt,transmission:Me,transmissionMap:P,thicknessMap:ae,gradientMap:j,opaque:b.transparent===!1&&b.blending===oo&&b.alphaToCoverage===!1,alphaMap:Z,alphaTest:de,alphaHash:ce,combine:b.combine,mapUv:nt&&v(b.map.channel),aoMapUv:k&&v(b.aoMap.channel),lightMapUv:wn&&v(b.lightMap.channel),bumpMapUv:Ye&&v(b.bumpMap.channel),normalMapUv:Ze&&v(b.normalMap.channel),displacementMapUv:Ce&&v(b.displacementMap.channel),emissiveMapUv:xt&&v(b.emissiveMap.channel),metalnessMapUv:Se&&v(b.metalnessMap.channel),roughnessMapUv:T&&v(b.roughnessMap.channel),anisotropyMapUv:le&&v(b.anisotropyMap.channel),clearcoatMapUv:he&&v(b.clearcoatMap.channel),clearcoatNormalMapUv:Qe&&v(b.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:ne&&v(b.clearcoatRoughnessMap.channel),iridescenceMapUv:pe&&v(b.iridescenceMap.channel),iridescenceThicknessMapUv:De&&v(b.iridescenceThicknessMap.channel),sheenColorMapUv:Ae&&v(b.sheenColorMap.channel),sheenRoughnessMapUv:me&&v(b.sheenRoughnessMap.channel),specularMapUv:Ke&&v(b.specularMap.channel),specularColorMapUv:ke&&v(b.specularColorMap.channel),specularIntensityMapUv:gt&&v(b.specularIntensityMap.channel),transmissionMapUv:P&&v(b.transmissionMap.channel),thicknessMapUv:ae&&v(b.thicknessMap.channel),alphaMapUv:Z&&v(b.alphaMap.channel),vertexTangents:!!$.attributes.tangent&&(Ze||E),vertexColors:b.vertexColors,vertexAlphas:b.vertexColors===!0&&!!$.attributes.color&&$.attributes.color.itemSize===4,pointsUvs:O.isPoints===!0&&!!$.attributes.uv&&(nt||Z),fog:!!U,useFog:b.fog===!0,fogExp2:!!U&&U.isFogExp2,flatShading:b.flatShading===!0,sizeAttenuation:b.sizeAttenuation===!0,logarithmicDepthBuffer:d,reverseDepthBuffer:be,skinning:O.isSkinnedMesh===!0,morphTargets:$.morphAttributes.position!==void 0,morphNormals:$.morphAttributes.normal!==void 0,morphColors:$.morphAttributes.color!==void 0,morphTargetsCount:oe,morphTextureStride:ge,numDirLights:x.directional.length,numPointLights:x.point.length,numSpotLights:x.spot.length,numSpotLightMaps:x.spotLightMap.length,numRectAreaLights:x.rectArea.length,numHemiLights:x.hemi.length,numDirLightShadows:x.directionalShadowMap.length,numPointLightShadows:x.pointShadowMap.length,numSpotLightShadows:x.spotShadowMap.length,numSpotLightShadowsWithMaps:x.numSpotLightShadowsWithMaps,numLightProbes:x.numLightProbes,numClippingPlanes:o.numPlanes,numClipIntersection:o.numIntersection,dithering:b.dithering,shadowMapEnabled:n.shadowMap.enabled&&D.length>0,shadowMapType:n.shadowMap.type,toneMapping:Tt,decodeVideoTexture:nt&&b.map.isVideoTexture===!0&&Xe.getTransfer(b.map.colorSpace)===ot,decodeVideoTextureEmissive:xt&&b.emissiveMap.isVideoTexture===!0&&Xe.getTransfer(b.emissiveMap.colorSpace)===ot,premultipliedAlpha:b.premultipliedAlpha,doubleSided:b.side===un,flipSided:b.side===Yt,useDepthPacking:b.depthPacking>=0,depthPacking:b.depthPacking||0,index0AttributeName:b.index0AttributeName,extensionClipCullDistance:Oe&&b.extensions.clipCullDistance===!0&&i.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(Oe&&b.extensions.multiDraw===!0||Ne)&&i.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:i.has("KHR_parallel_shader_compile"),customProgramCacheKey:b.customProgramCacheKey()};return Wt.vertexUv1s=c.has(1),Wt.vertexUv2s=c.has(2),Wt.vertexUv3s=c.has(3),c.clear(),Wt}function p(b){let x=[];if(b.shaderID?x.push(b.shaderID):(x.push(b.customVertexShaderID),x.push(b.customFragmentShaderID)),b.defines!==void 0)for(let D in b.defines)x.push(D),x.push(b.defines[D]);return b.isRawShaderMaterial===!1&&(M(x,b),_(x,b),x.push(n.outputColorSpace)),x.push(b.customProgramCacheKey),x.join()}function M(b,x){b.push(x.precision),b.push(x.outputColorSpace),b.push(x.envMapMode),b.push(x.envMapCubeUVHeight),b.push(x.mapUv),b.push(x.alphaMapUv),b.push(x.lightMapUv),b.push(x.aoMapUv),b.push(x.bumpMapUv),b.push(x.normalMapUv),b.push(x.displacementMapUv),b.push(x.emissiveMapUv),b.push(x.metalnessMapUv),b.push(x.roughnessMapUv),b.push(x.anisotropyMapUv),b.push(x.clearcoatMapUv),b.push(x.clearcoatNormalMapUv),b.push(x.clearcoatRoughnessMapUv),b.push(x.iridescenceMapUv),b.push(x.iridescenceThicknessMapUv),b.push(x.sheenColorMapUv),b.push(x.sheenRoughnessMapUv),b.push(x.specularMapUv),b.push(x.specularColorMapUv),b.push(x.specularIntensityMapUv),b.push(x.transmissionMapUv),b.push(x.thicknessMapUv),b.push(x.combine),b.push(x.fogExp2),b.push(x.sizeAttenuation),b.push(x.morphTargetsCount),b.push(x.morphAttributeCount),b.push(x.numDirLights),b.push(x.numPointLights),b.push(x.numSpotLights),b.push(x.numSpotLightMaps),b.push(x.numHemiLights),b.push(x.numRectAreaLights),b.push(x.numDirLightShadows),b.push(x.numPointLightShadows),b.push(x.numSpotLightShadows),b.push(x.numSpotLightShadowsWithMaps),b.push(x.numLightProbes),b.push(x.shadowMapType),b.push(x.toneMapping),b.push(x.numClippingPlanes),b.push(x.numClipIntersection),b.push(x.depthPacking)}function _(b,x){a.disableAll(),x.supportsVertexTextures&&a.enable(0),x.instancing&&a.enable(1),x.instancingColor&&a.enable(2),x.instancingMorph&&a.enable(3),x.matcap&&a.enable(4),x.envMap&&a.enable(5),x.normalMapObjectSpace&&a.enable(6),x.normalMapTangentSpace&&a.enable(7),x.clearcoat&&a.enable(8),x.iridescence&&a.enable(9),x.alphaTest&&a.enable(10),x.vertexColors&&a.enable(11),x.vertexAlphas&&a.enable(12),x.vertexUv1s&&a.enable(13),x.vertexUv2s&&a.enable(14),x.vertexUv3s&&a.enable(15),x.vertexTangents&&a.enable(16),x.anisotropy&&a.enable(17),x.alphaHash&&a.enable(18),x.batching&&a.enable(19),x.dispersion&&a.enable(20),x.batchingColor&&a.enable(21),b.push(a.mask),a.disableAll(),x.fog&&a.enable(0),x.useFog&&a.enable(1),x.flatShading&&a.enable(2),x.logarithmicDepthBuffer&&a.enable(3),x.reverseDepthBuffer&&a.enable(4),x.skinning&&a.enable(5),x.morphTargets&&a.enable(6),x.morphNormals&&a.enable(7),x.morphColors&&a.enable(8),x.premultipliedAlpha&&a.enable(9),x.shadowMapEnabled&&a.enable(10),x.doubleSided&&a.enable(11),x.flipSided&&a.enable(12),x.useDepthPacking&&a.enable(13),x.dithering&&a.enable(14),x.transmission&&a.enable(15),x.sheen&&a.enable(16),x.opaque&&a.enable(17),x.pointsUvs&&a.enable(18),x.decodeVideoTexture&&a.enable(19),x.decodeVideoTextureEmissive&&a.enable(20),x.alphaToCoverage&&a.enable(21),b.push(a.mask)}function y(b){let x=m[b.type],D;if(x){let N=ui[x];D=Za.clone(N.uniforms)}else D=b.uniforms;return D}function A(b,x){let D;for(let N=0,O=u.length;N<O;N++){let U=u[N];if(U.cacheKey===x){D=U,++D.usedTimes;break}}return D===void 0&&(D=new fO(n,x,b,s),u.push(D)),D}function C(b){if(--b.usedTimes===0){let x=u.indexOf(b);u[x]=u[u.length-1],u.pop(),b.destroy()}}function S(b){l.remove(b)}function I(){l.dispose()}return{getParameters:g,getProgramCacheKey:p,getUniforms:y,acquireProgram:A,releaseProgram:C,releaseShaderCache:S,programs:u,dispose:I}}function mO(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,l){n.get(o)[a]=l}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function gO(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.z!==e.z?n.z-e.z:n.id-e.id}function bM(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function wM(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d,h,f,m,v,g){let p=n[e];return p===void 0?(p={id:d.id,object:d,geometry:h,material:f,groupOrder:m,renderOrder:d.renderOrder,z:v,group:g},n[e]=p):(p.id=d.id,p.object=d,p.geometry=h,p.material=f,p.groupOrder=m,p.renderOrder=d.renderOrder,p.z=v,p.group=g),e++,p}function a(d,h,f,m,v,g){let p=o(d,h,f,m,v,g);f.transmission>0?i.push(p):f.transparent===!0?r.push(p):t.push(p)}function l(d,h,f,m,v,g){let p=o(d,h,f,m,v,g);f.transmission>0?i.unshift(p):f.transparent===!0?r.unshift(p):t.unshift(p)}function c(d,h){t.length>1&&t.sort(d||gO),i.length>1&&i.sort(h||bM),r.length>1&&r.sort(h||bM)}function u(){for(let d=e,h=n.length;d<h;d++){let f=n[d];if(f.id===null)break;f.id=null,f.object=null,f.geometry=null,f.material=null,f.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:a,unshift:l,finish:u,sort:c}}function yO(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new wM,n.set(i,[o])):r>=s.length?(o=new wM,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function vO(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new R,color:new xe};break;case"SpotLight":t={position:new R,direction:new R,color:new xe,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new R,color:new xe,distance:0,decay:0};break;case"HemisphereLight":t={direction:new R,skyColor:new xe,groundColor:new xe};break;case"RectAreaLight":t={color:new xe,position:new R,halfWidth:new R,halfHeight:new R};break}return n[e.id]=t,t}}}function _O(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new te};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new te};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new te,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var xO=0;function MO(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function EO(n){let e=new vO,t=_O(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let c=0;c<9;c++)i.probe.push(new R);let r=new R,s=new tt,o=new tt;function a(c){let u=0,d=0,h=0;for(let b=0;b<9;b++)i.probe[b].set(0,0,0);let f=0,m=0,v=0,g=0,p=0,M=0,_=0,y=0,A=0,C=0,S=0;c.sort(MO);for(let b=0,x=c.length;b<x;b++){let D=c[b],N=D.color,O=D.intensity,U=D.distance,$=D.shadow&&D.shadow.map?D.shadow.map.texture:null;if(D.isAmbientLight)u+=N.r*O,d+=N.g*O,h+=N.b*O;else if(D.isLightProbe){for(let W=0;W<9;W++)i.probe[W].addScaledVector(D.sh.coefficients[W],O);S++}else if(D.isDirectionalLight){let W=e.get(D);if(W.color.copy(D.color).multiplyScalar(D.intensity),D.castShadow){let q=D.shadow,z=t.get(D);z.shadowIntensity=q.intensity,z.shadowBias=q.bias,z.shadowNormalBias=q.normalBias,z.shadowRadius=q.radius,z.shadowMapSize=q.mapSize,i.directionalShadow[f]=z,i.directionalShadowMap[f]=$,i.directionalShadowMatrix[f]=D.shadow.matrix,M++}i.directional[f]=W,f++}else if(D.isSpotLight){let W=e.get(D);W.position.setFromMatrixPosition(D.matrixWorld),W.color.copy(N).multiplyScalar(O),W.distance=U,W.coneCos=Math.cos(D.angle),W.penumbraCos=Math.cos(D.angle*(1-D.penumbra)),W.decay=D.decay,i.spot[v]=W;let q=D.shadow;if(D.map&&(i.spotLightMap[A]=D.map,A++,q.updateMatrices(D),D.castShadow&&C++),i.spotLightMatrix[v]=q.matrix,D.castShadow){let z=t.get(D);z.shadowIntensity=q.intensity,z.shadowBias=q.bias,z.shadowNormalBias=q.normalBias,z.shadowRadius=q.radius,z.shadowMapSize=q.mapSize,i.spotShadow[v]=z,i.spotShadowMap[v]=$,y++}v++}else if(D.isRectAreaLight){let W=e.get(D);W.color.copy(N).multiplyScalar(O),W.halfWidth.set(D.width*.5,0,0),W.halfHeight.set(0,D.height*.5,0),i.rectArea[g]=W,g++}else if(D.isPointLight){let W=e.get(D);if(W.color.copy(D.color).multiplyScalar(D.intensity),W.distance=D.distance,W.decay=D.decay,D.castShadow){let q=D.shadow,z=t.get(D);z.shadowIntensity=q.intensity,z.shadowBias=q.bias,z.shadowNormalBias=q.normalBias,z.shadowRadius=q.radius,z.shadowMapSize=q.mapSize,z.shadowCameraNear=q.camera.near,z.shadowCameraFar=q.camera.far,i.pointShadow[m]=z,i.pointShadowMap[m]=$,i.pointShadowMatrix[m]=D.shadow.matrix,_++}i.point[m]=W,m++}else if(D.isHemisphereLight){let W=e.get(D);W.skyColor.copy(D.color).multiplyScalar(O),W.groundColor.copy(D.groundColor).multiplyScalar(O),i.hemi[p]=W,p++}}g>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=se.LTC_FLOAT_1,i.rectAreaLTC2=se.LTC_FLOAT_2):(i.rectAreaLTC1=se.LTC_HALF_1,i.rectAreaLTC2=se.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=h;let I=i.hash;(I.directionalLength!==f||I.pointLength!==m||I.spotLength!==v||I.rectAreaLength!==g||I.hemiLength!==p||I.numDirectionalShadows!==M||I.numPointShadows!==_||I.numSpotShadows!==y||I.numSpotMaps!==A||I.numLightProbes!==S)&&(i.directional.length=f,i.spot.length=v,i.rectArea.length=g,i.point.length=m,i.hemi.length=p,i.directionalShadow.length=M,i.directionalShadowMap.length=M,i.pointShadow.length=_,i.pointShadowMap.length=_,i.spotShadow.length=y,i.spotShadowMap.length=y,i.directionalShadowMatrix.length=M,i.pointShadowMatrix.length=_,i.spotLightMatrix.length=y+A-C,i.spotLightMap.length=A,i.numSpotLightShadowsWithMaps=C,i.numLightProbes=S,I.directionalLength=f,I.pointLength=m,I.spotLength=v,I.rectAreaLength=g,I.hemiLength=p,I.numDirectionalShadows=M,I.numPointShadows=_,I.numSpotShadows=y,I.numSpotMaps=A,I.numLightProbes=S,i.version=xO++)}function l(c,u){let d=0,h=0,f=0,m=0,v=0,g=u.matrixWorldInverse;for(let p=0,M=c.length;p<M;p++){let _=c[p];if(_.isDirectionalLight){let y=i.directional[d];y.direction.setFromMatrixPosition(_.matrixWorld),r.setFromMatrixPosition(_.target.matrixWorld),y.direction.sub(r),y.direction.transformDirection(g),d++}else if(_.isSpotLight){let y=i.spot[f];y.position.setFromMatrixPosition(_.matrixWorld),y.position.applyMatrix4(g),y.direction.setFromMatrixPosition(_.matrixWorld),r.setFromMatrixPosition(_.target.matrixWorld),y.direction.sub(r),y.direction.transformDirection(g),f++}else if(_.isRectAreaLight){let y=i.rectArea[m];y.position.setFromMatrixPosition(_.matrixWorld),y.position.applyMatrix4(g),o.identity(),s.copy(_.matrixWorld),s.premultiply(g),o.extractRotation(s),y.halfWidth.set(_.width*.5,0,0),y.halfHeight.set(0,_.height*.5,0),y.halfWidth.applyMatrix4(o),y.halfHeight.applyMatrix4(o),m++}else if(_.isPointLight){let y=i.point[h];y.position.setFromMatrixPosition(_.matrixWorld),y.position.applyMatrix4(g),h++}else if(_.isHemisphereLight){let y=i.hemi[v];y.direction.setFromMatrixPosition(_.matrixWorld),y.direction.transformDirection(g),v++}}}return{setup:a,setupView:l,state:i}}function SM(n){let e=new EO(n),t=[],i=[];function r(u){c.camera=u,t.length=0,i.length=0}function s(u){t.push(u)}function o(u){i.push(u)}function a(){e.setup(t)}function l(u){e.setupView(t,u)}let c={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:c,setupLights:a,setupLightsView:l,pushLight:s,pushShadow:o}}function bO(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new SM(n),e.set(r,[a])):s>=o.length?(a=new SM(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var Ym=class extends Hi{static get type(){return"MeshDepthMaterial"}constructor(e){super(),this.isMeshDepthMaterial=!0,this.depthPacking=gI,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Zm=class extends Hi{static get type(){return"MeshDistanceMaterial"}constructor(e){super(),this.isMeshDistanceMaterial=!0,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}},wO=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,SO=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
#include <packing>
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = unpackRGBATo2Half( texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ) );
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = unpackRGBAToDepth( texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ) );
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( squared_mean - mean * mean );
	gl_FragColor = pack2HalfToRGBA( vec2( mean, std_dev ) );
}`;function CO(n,e,t){let i=new Ba,r=new te,s=new te,o=new At,a=new Ym({depthPacking:yI}),l=new Zm,c={},u=t.maxTextureSize,d={[cr]:Yt,[Yt]:cr,[un]:un},h=new Gt({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new te},radius:{value:4}},vertexShader:wO,fragmentShader:SO}),f=h.clone();f.defines.HORIZONTAL_PASS=1;let m=new zt;m.setAttribute("position",new Zt(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let v=new wt(m,h),g=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=VM;let p=this.type;this.render=function(C,S,I){if(g.enabled===!1||g.autoUpdate===!1&&g.needsUpdate===!1||C.length===0)return;let b=n.getRenderTarget(),x=n.getActiveCubeFace(),D=n.getActiveMipmapLevel(),N=n.state;N.setBlending(hi),N.buffers.color.setClear(1,1,1,1),N.buffers.depth.setTest(!0),N.setScissorTest(!1);let O=p!==Li&&this.type===Li,U=p===Li&&this.type!==Li;for(let $=0,W=C.length;$<W;$++){let q=C[$],z=q.shadow;if(z===void 0){console.warn("THREE.WebGLShadowMap:",q,"has no shadow.");continue}if(z.autoUpdate===!1&&z.needsUpdate===!1)continue;r.copy(z.mapSize);let Q=z.getFrameExtents();if(r.multiply(Q),s.copy(z.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/Q.x),r.x=s.x*Q.x,z.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/Q.y),r.y=s.y*Q.y,z.mapSize.y=s.y)),z.map===null||O===!0||U===!0){let oe=this.type!==Li?{minFilter:_n,magFilter:_n}:{};z.map!==null&&z.map.dispose(),z.map=new xn(r.x,r.y,oe),z.map.texture.name=q.name+".shadowMap",z.camera.updateProjectionMatrix()}n.setRenderTarget(z.map),n.clear();let ee=z.getViewportCount();for(let oe=0;oe<ee;oe++){let ge=z.getViewport(oe);o.set(s.x*ge.x,s.y*ge.y,s.x*ge.z,s.y*ge.w),N.viewport(o),z.updateMatrices(q,oe),i=z.getFrustum(),y(S,I,z.camera,q,this.type)}z.isPointLightShadow!==!0&&this.type===Li&&M(z,I),z.needsUpdate=!1}p=this.type,g.needsUpdate=!1,n.setRenderTarget(b,x,D)};function M(C,S){let I=e.update(v);h.defines.VSM_SAMPLES!==C.blurSamples&&(h.defines.VSM_SAMPLES=C.blurSamples,f.defines.VSM_SAMPLES=C.blurSamples,h.needsUpdate=!0,f.needsUpdate=!0),C.mapPass===null&&(C.mapPass=new xn(r.x,r.y)),h.uniforms.shadow_pass.value=C.map.texture,h.uniforms.resolution.value=C.mapSize,h.uniforms.radius.value=C.radius,n.setRenderTarget(C.mapPass),n.clear(),n.renderBufferDirect(S,null,I,h,v,null),f.uniforms.shadow_pass.value=C.mapPass.texture,f.uniforms.resolution.value=C.mapSize,f.uniforms.radius.value=C.radius,n.setRenderTarget(C.map),n.clear(),n.renderBufferDirect(S,null,I,f,v,null)}function _(C,S,I,b){let x=null,D=I.isPointLight===!0?C.customDistanceMaterial:C.customDepthMaterial;if(D!==void 0)x=D;else if(x=I.isPointLight===!0?l:a,n.localClippingEnabled&&S.clipShadows===!0&&Array.isArray(S.clippingPlanes)&&S.clippingPlanes.length!==0||S.displacementMap&&S.displacementScale!==0||S.alphaMap&&S.alphaTest>0||S.map&&S.alphaTest>0){let N=x.uuid,O=S.uuid,U=c[N];U===void 0&&(U={},c[N]=U);let $=U[O];$===void 0&&($=x.clone(),U[O]=$,S.addEventListener("dispose",A)),x=$}if(x.visible=S.visible,x.wireframe=S.wireframe,b===Li?x.side=S.shadowSide!==null?S.shadowSide:S.side:x.side=S.shadowSide!==null?S.shadowSide:d[S.side],x.alphaMap=S.alphaMap,x.alphaTest=S.alphaTest,x.map=S.map,x.clipShadows=S.clipShadows,x.clippingPlanes=S.clippingPlanes,x.clipIntersection=S.clipIntersection,x.displacementMap=S.displacementMap,x.displacementScale=S.displacementScale,x.displacementBias=S.displacementBias,x.wireframeLinewidth=S.wireframeLinewidth,x.linewidth=S.linewidth,I.isPointLight===!0&&x.isMeshDistanceMaterial===!0){let N=n.properties.get(x);N.light=I}return x}function y(C,S,I,b,x){if(C.visible===!1)return;if(C.layers.test(S.layers)&&(C.isMesh||C.isLine||C.isPoints)&&(C.castShadow||C.receiveShadow&&x===Li)&&(!C.frustumCulled||i.intersectsObject(C))){C.modelViewMatrix.multiplyMatrices(I.matrixWorldInverse,C.matrixWorld);let O=e.update(C),U=C.material;if(Array.isArray(U)){let $=O.groups;for(let W=0,q=$.length;W<q;W++){let z=$[W],Q=U[z.materialIndex];if(Q&&Q.visible){let ee=_(C,Q,b,x);C.onBeforeShadow(n,C,S,I,O,ee,z),n.renderBufferDirect(I,null,O,ee,C,z),C.onAfterShadow(n,C,S,I,O,ee,z)}}}else if(U.visible){let $=_(C,U,b,x);C.onBeforeShadow(n,C,S,I,O,$,null),n.renderBufferDirect(I,null,O,$,C,null),C.onAfterShadow(n,C,S,I,O,$,null)}}let N=C.children;for(let O=0,U=N.length;O<U;O++)y(N[O],S,I,b,x)}function A(C){C.target.removeEventListener("dispose",A);for(let I in c){let b=c[I],x=C.target.uuid;x in b&&(b[x].dispose(),delete b[x])}}}var TO={[Qp]:em,[tm]:rm,[nm]:sm,[uo]:im,[em]:Qp,[rm]:tm,[sm]:nm,[im]:uo};function DO(n,e){function t(){let P=!1,ae=new At,j=null,Z=new At(0,0,0,0);return{setMask:function(de){j!==de&&!P&&(n.colorMask(de,de,de,de),j=de)},setLocked:function(de){P=de},setClear:function(de,ce,Oe,Tt,Wt){Wt===!0&&(de*=Tt,ce*=Tt,Oe*=Tt),ae.set(de,ce,Oe,Tt),Z.equals(ae)===!1&&(n.clearColor(de,ce,Oe,Tt),Z.copy(ae))},reset:function(){P=!1,j=null,Z.set(-1,0,0,0)}}}function i(){let P=!1,ae=!1,j=null,Z=null,de=null;return{setReversed:function(ce){if(ae!==ce){let Oe=e.get("EXT_clip_control");ae?Oe.clipControlEXT(Oe.LOWER_LEFT_EXT,Oe.ZERO_TO_ONE_EXT):Oe.clipControlEXT(Oe.LOWER_LEFT_EXT,Oe.NEGATIVE_ONE_TO_ONE_EXT);let Tt=de;de=null,this.setClear(Tt)}ae=ce},getReversed:function(){return ae},setTest:function(ce){ce?re(n.DEPTH_TEST):be(n.DEPTH_TEST)},setMask:function(ce){j!==ce&&!P&&(n.depthMask(ce),j=ce)},setFunc:function(ce){if(ae&&(ce=TO[ce]),Z!==ce){switch(ce){case Qp:n.depthFunc(n.NEVER);break;case em:n.depthFunc(n.ALWAYS);break;case tm:n.depthFunc(n.LESS);break;case uo:n.depthFunc(n.LEQUAL);break;case nm:n.depthFunc(n.EQUAL);break;case im:n.depthFunc(n.GEQUAL);break;case rm:n.depthFunc(n.GREATER);break;case sm:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}Z=ce}},setLocked:function(ce){P=ce},setClear:function(ce){de!==ce&&(ae&&(ce=1-ce),n.clearDepth(ce),de=ce)},reset:function(){P=!1,j=null,Z=null,de=null,ae=!1}}}function r(){let P=!1,ae=null,j=null,Z=null,de=null,ce=null,Oe=null,Tt=null,Wt=null;return{setTest:function(at){P||(at?re(n.STENCIL_TEST):be(n.STENCIL_TEST))},setMask:function(at){ae!==at&&!P&&(n.stencilMask(at),ae=at)},setFunc:function(at,On,yi){(j!==at||Z!==On||de!==yi)&&(n.stencilFunc(at,On,yi),j=at,Z=On,de=yi)},setOp:function(at,On,yi){(ce!==at||Oe!==On||Tt!==yi)&&(n.stencilOp(at,On,yi),ce=at,Oe=On,Tt=yi)},setLocked:function(at){P=at},setClear:function(at){Wt!==at&&(n.clearStencil(at),Wt=at)},reset:function(){P=!1,ae=null,j=null,Z=null,de=null,ce=null,Oe=null,Tt=null,Wt=null}}}let s=new t,o=new i,a=new r,l=new WeakMap,c=new WeakMap,u={},d={},h=new WeakMap,f=[],m=null,v=!1,g=null,p=null,M=null,_=null,y=null,A=null,C=null,S=new xe(0,0,0),I=0,b=!1,x=null,D=null,N=null,O=null,U=null,$=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),W=!1,q=0,z=n.getParameter(n.VERSION);z.indexOf("WebGL")!==-1?(q=parseFloat(/^WebGL (\d)/.exec(z)[1]),W=q>=1):z.indexOf("OpenGL ES")!==-1&&(q=parseFloat(/^OpenGL ES (\d)/.exec(z)[1]),W=q>=2);let Q=null,ee={},oe=n.getParameter(n.SCISSOR_BOX),ge=n.getParameter(n.VIEWPORT),we=new At().fromArray(oe),G=new At().fromArray(ge);function J(P,ae,j,Z){let de=new Uint8Array(4),ce=n.createTexture();n.bindTexture(P,ce),n.texParameteri(P,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(P,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let Oe=0;Oe<j;Oe++)P===n.TEXTURE_3D||P===n.TEXTURE_2D_ARRAY?n.texImage3D(ae,0,n.RGBA,1,1,Z,0,n.RGBA,n.UNSIGNED_BYTE,de):n.texImage2D(ae+Oe,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,de);return ce}let fe={};fe[n.TEXTURE_2D]=J(n.TEXTURE_2D,n.TEXTURE_2D,1),fe[n.TEXTURE_CUBE_MAP]=J(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),fe[n.TEXTURE_2D_ARRAY]=J(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),fe[n.TEXTURE_3D]=J(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),re(n.DEPTH_TEST),o.setFunc(uo),Ye(!1),Ze(Cx),re(n.CULL_FACE),k(hi);function re(P){u[P]!==!0&&(n.enable(P),u[P]=!0)}function be(P){u[P]!==!1&&(n.disable(P),u[P]=!1)}function Te(P,ae){return d[P]!==ae?(n.bindFramebuffer(P,ae),d[P]=ae,P===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=ae),P===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=ae),!0):!1}function Ne(P,ae){let j=f,Z=!1;if(P){j=h.get(ae),j===void 0&&(j=[],h.set(ae,j));let de=P.textures;if(j.length!==de.length||j[0]!==n.COLOR_ATTACHMENT0){for(let ce=0,Oe=de.length;ce<Oe;ce++)j[ce]=n.COLOR_ATTACHMENT0+ce;j.length=de.length,Z=!0}}else j[0]!==n.BACK&&(j[0]=n.BACK,Z=!0);Z&&n.drawBuffers(j)}function nt(P){return m!==P?(n.useProgram(P),m=P,!0):!1}let ze={[Br]:n.FUNC_ADD,[YA]:n.FUNC_SUBTRACT,[ZA]:n.FUNC_REVERSE_SUBTRACT};ze[KA]=n.MIN,ze[JA]=n.MAX;let _t={[QA]:n.ZERO,[eI]:n.ONE,[tI]:n.SRC_COLOR,[Kp]:n.SRC_ALPHA,[aI]:n.SRC_ALPHA_SATURATE,[sI]:n.DST_COLOR,[iI]:n.DST_ALPHA,[nI]:n.ONE_MINUS_SRC_COLOR,[Jp]:n.ONE_MINUS_SRC_ALPHA,[oI]:n.ONE_MINUS_DST_COLOR,[rI]:n.ONE_MINUS_DST_ALPHA,[lI]:n.CONSTANT_COLOR,[cI]:n.ONE_MINUS_CONSTANT_COLOR,[uI]:n.CONSTANT_ALPHA,[dI]:n.ONE_MINUS_CONSTANT_ALPHA};function k(P,ae,j,Z,de,ce,Oe,Tt,Wt,at){if(P===hi){v===!0&&(be(n.BLEND),v=!1);return}if(v===!1&&(re(n.BLEND),v=!0),P!==XA){if(P!==g||at!==b){if((p!==Br||y!==Br)&&(n.blendEquation(n.FUNC_ADD),p=Br,y=Br),at)switch(P){case oo:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Tx:n.blendFunc(n.ONE,n.ONE);break;case Dx:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Ax:n.blendFuncSeparate(n.ZERO,n.SRC_COLOR,n.ZERO,n.SRC_ALPHA);break;default:console.error("THREE.WebGLState: Invalid blending: ",P);break}else switch(P){case oo:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Tx:n.blendFunc(n.SRC_ALPHA,n.ONE);break;case Dx:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Ax:n.blendFunc(n.ZERO,n.SRC_COLOR);break;default:console.error("THREE.WebGLState: Invalid blending: ",P);break}M=null,_=null,A=null,C=null,S.set(0,0,0),I=0,g=P,b=at}return}de=de||ae,ce=ce||j,Oe=Oe||Z,(ae!==p||de!==y)&&(n.blendEquationSeparate(ze[ae],ze[de]),p=ae,y=de),(j!==M||Z!==_||ce!==A||Oe!==C)&&(n.blendFuncSeparate(_t[j],_t[Z],_t[ce],_t[Oe]),M=j,_=Z,A=ce,C=Oe),(Tt.equals(S)===!1||Wt!==I)&&(n.blendColor(Tt.r,Tt.g,Tt.b,Wt),S.copy(Tt),I=Wt),g=P,b=!1}function wn(P,ae){P.side===un?be(n.CULL_FACE):re(n.CULL_FACE);let j=P.side===Yt;ae&&(j=!j),Ye(j),P.blending===oo&&P.transparent===!1?k(hi):k(P.blending,P.blendEquation,P.blendSrc,P.blendDst,P.blendEquationAlpha,P.blendSrcAlpha,P.blendDstAlpha,P.blendColor,P.blendAlpha,P.premultipliedAlpha),o.setFunc(P.depthFunc),o.setTest(P.depthTest),o.setMask(P.depthWrite),s.setMask(P.colorWrite);let Z=P.stencilWrite;a.setTest(Z),Z&&(a.setMask(P.stencilWriteMask),a.setFunc(P.stencilFunc,P.stencilRef,P.stencilFuncMask),a.setOp(P.stencilFail,P.stencilZFail,P.stencilZPass)),xt(P.polygonOffset,P.polygonOffsetFactor,P.polygonOffsetUnits),P.alphaToCoverage===!0?re(n.SAMPLE_ALPHA_TO_COVERAGE):be(n.SAMPLE_ALPHA_TO_COVERAGE)}function Ye(P){x!==P&&(P?n.frontFace(n.CW):n.frontFace(n.CCW),x=P)}function Ze(P){P!==jA?(re(n.CULL_FACE),P!==D&&(P===Cx?n.cullFace(n.BACK):P===$A?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):be(n.CULL_FACE),D=P}function Ce(P){P!==N&&(W&&n.lineWidth(P),N=P)}function xt(P,ae,j){P?(re(n.POLYGON_OFFSET_FILL),(O!==ae||U!==j)&&(n.polygonOffset(ae,j),O=ae,U=j)):be(n.POLYGON_OFFSET_FILL)}function Se(P){P?re(n.SCISSOR_TEST):be(n.SCISSOR_TEST)}function T(P){P===void 0&&(P=n.TEXTURE0+$-1),Q!==P&&(n.activeTexture(P),Q=P)}function E(P,ae,j){j===void 0&&(Q===null?j=n.TEXTURE0+$-1:j=Q);let Z=ee[j];Z===void 0&&(Z={type:void 0,texture:void 0},ee[j]=Z),(Z.type!==P||Z.texture!==ae)&&(Q!==j&&(n.activeTexture(j),Q=j),n.bindTexture(P,ae||fe[P]),Z.type=P,Z.texture=ae)}function V(){let P=ee[Q];P!==void 0&&P.type!==void 0&&(n.bindTexture(P.type,null),P.type=void 0,P.texture=void 0)}function Y(){try{n.compressedTexImage2D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function K(){try{n.compressedTexImage3D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function X(){try{n.texSubImage2D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function Me(){try{n.texSubImage3D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function le(){try{n.compressedTexSubImage2D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function he(){try{n.compressedTexSubImage3D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function Qe(){try{n.texStorage2D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function ne(){try{n.texStorage3D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function pe(){try{n.texImage2D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function De(){try{n.texImage3D.apply(n,arguments)}catch(P){console.error("THREE.WebGLState:",P)}}function Ae(P){we.equals(P)===!1&&(n.scissor(P.x,P.y,P.z,P.w),we.copy(P))}function me(P){G.equals(P)===!1&&(n.viewport(P.x,P.y,P.z,P.w),G.copy(P))}function Ke(P,ae){let j=c.get(ae);j===void 0&&(j=new WeakMap,c.set(ae,j));let Z=j.get(P);Z===void 0&&(Z=n.getUniformBlockIndex(ae,P.name),j.set(P,Z))}function ke(P,ae){let Z=c.get(ae).get(P);l.get(ae)!==Z&&(n.uniformBlockBinding(ae,Z,P.__bindingPointIndex),l.set(ae,Z))}function gt(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),u={},Q=null,ee={},d={},h=new WeakMap,f=[],m=null,v=!1,g=null,p=null,M=null,_=null,y=null,A=null,C=null,S=new xe(0,0,0),I=0,b=!1,x=null,D=null,N=null,O=null,U=null,we.set(0,0,n.canvas.width,n.canvas.height),G.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:re,disable:be,bindFramebuffer:Te,drawBuffers:Ne,useProgram:nt,setBlending:k,setMaterial:wn,setFlipSided:Ye,setCullFace:Ze,setLineWidth:Ce,setPolygonOffset:xt,setScissorTest:Se,activeTexture:T,bindTexture:E,unbindTexture:V,compressedTexImage2D:Y,compressedTexImage3D:K,texImage2D:pe,texImage3D:De,updateUBOMapping:Ke,uniformBlockBinding:ke,texStorage2D:Qe,texStorage3D:ne,texSubImage2D:X,texSubImage3D:Me,compressedTexSubImage2D:le,compressedTexSubImage3D:he,scissor:Ae,viewport:me,reset:gt}}function CM(n,e,t,i){let r=AO(i);switch(t){case GM:return n*e;case jM:return n*e;case $M:return n*e*2;case Ig:return n*e/r.components*r.byteLength;case Rg:return n*e/r.components*r.byteLength;case qM:return n*e*2/r.components*r.byteLength;case Ng:return n*e*2/r.components*r.byteLength;case WM:return n*e*3/r.components*r.byteLength;case $n:return n*e*4/r.components*r.byteLength;case Pg:return n*e*4/r.components*r.byteLength;case vu:case _u:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case xu:case Mu:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case dm:case hm:return Math.max(n,16)*Math.max(e,8)/4;case um:case fm:return Math.max(n,8)*Math.max(e,8)/2;case pm:case mm:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case gm:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case ym:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case vm:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case _m:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case xm:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case Mm:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Em:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case bm:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case wm:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Sm:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Cm:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Tm:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Dm:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case Am:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Im:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Eu:case Rm:case Nm:return Math.ceil(n/4)*Math.ceil(e/4)*16;case XM:case Pm:return Math.ceil(n/4)*Math.ceil(e/4)*8;case Om:case Lm:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function AO(n){switch(n){case Ui:case BM:return{byteLength:1,components:1};case ka:case HM:case ns:return{byteLength:2,components:1};case Dg:case Ag:return{byteLength:2,components:4};case Wr:case Tg:case di:return{byteLength:4,components:1};case zM:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}function IO(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,l=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),c=new te,u=new WeakMap,d,h=new WeakMap,f=!1;try{f=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function m(T,E){return f?new OffscreenCanvas(T,E):Su("canvas")}function v(T,E,V){let Y=1,K=Se(T);if((K.width>V||K.height>V)&&(Y=V/Math.max(K.width,K.height)),Y<1)if(typeof HTMLImageElement<"u"&&T instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&T instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&T instanceof ImageBitmap||typeof VideoFrame<"u"&&T instanceof VideoFrame){let X=Math.floor(Y*K.width),Me=Math.floor(Y*K.height);d===void 0&&(d=m(X,Me));let le=E?m(X,Me):d;return le.width=X,le.height=Me,le.getContext("2d").drawImage(T,0,0,X,Me),console.warn("THREE.WebGLRenderer: Texture has been resized from ("+K.width+"x"+K.height+") to ("+X+"x"+Me+")."),le}else return"data"in T&&console.warn("THREE.WebGLRenderer: Image in DataTexture is too big ("+K.width+"x"+K.height+")."),T;return T}function g(T){return T.generateMipmaps}function p(T){n.generateMipmap(T)}function M(T){return T.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:T.isWebGL3DRenderTarget?n.TEXTURE_3D:T.isWebGLArrayRenderTarget||T.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function _(T,E,V,Y,K=!1){if(T!==null){if(n[T]!==void 0)return n[T];console.warn("THREE.WebGLRenderer: Attempt to use non-existing WebGL internal format '"+T+"'")}let X=E;if(E===n.RED&&(V===n.FLOAT&&(X=n.R32F),V===n.HALF_FLOAT&&(X=n.R16F),V===n.UNSIGNED_BYTE&&(X=n.R8)),E===n.RED_INTEGER&&(V===n.UNSIGNED_BYTE&&(X=n.R8UI),V===n.UNSIGNED_SHORT&&(X=n.R16UI),V===n.UNSIGNED_INT&&(X=n.R32UI),V===n.BYTE&&(X=n.R8I),V===n.SHORT&&(X=n.R16I),V===n.INT&&(X=n.R32I)),E===n.RG&&(V===n.FLOAT&&(X=n.RG32F),V===n.HALF_FLOAT&&(X=n.RG16F),V===n.UNSIGNED_BYTE&&(X=n.RG8)),E===n.RG_INTEGER&&(V===n.UNSIGNED_BYTE&&(X=n.RG8UI),V===n.UNSIGNED_SHORT&&(X=n.RG16UI),V===n.UNSIGNED_INT&&(X=n.RG32UI),V===n.BYTE&&(X=n.RG8I),V===n.SHORT&&(X=n.RG16I),V===n.INT&&(X=n.RG32I)),E===n.RGB_INTEGER&&(V===n.UNSIGNED_BYTE&&(X=n.RGB8UI),V===n.UNSIGNED_SHORT&&(X=n.RGB16UI),V===n.UNSIGNED_INT&&(X=n.RGB32UI),V===n.BYTE&&(X=n.RGB8I),V===n.SHORT&&(X=n.RGB16I),V===n.INT&&(X=n.RGB32I)),E===n.RGBA_INTEGER&&(V===n.UNSIGNED_BYTE&&(X=n.RGBA8UI),V===n.UNSIGNED_SHORT&&(X=n.RGBA16UI),V===n.UNSIGNED_INT&&(X=n.RGBA32UI),V===n.BYTE&&(X=n.RGBA8I),V===n.SHORT&&(X=n.RGBA16I),V===n.INT&&(X=n.RGBA32I)),E===n.RGB&&V===n.UNSIGNED_INT_5_9_9_9_REV&&(X=n.RGB9_E5),E===n.RGBA){let Me=K?td:Xe.getTransfer(Y);V===n.FLOAT&&(X=n.RGBA32F),V===n.HALF_FLOAT&&(X=n.RGBA16F),V===n.UNSIGNED_BYTE&&(X=Me===ot?n.SRGB8_ALPHA8:n.RGBA8),V===n.UNSIGNED_SHORT_4_4_4_4&&(X=n.RGBA4),V===n.UNSIGNED_SHORT_5_5_5_1&&(X=n.RGB5_A1)}return(X===n.R16F||X===n.R32F||X===n.RG16F||X===n.RG32F||X===n.RGBA16F||X===n.RGBA32F)&&e.get("EXT_color_buffer_float"),X}function y(T,E){let V;return T?E===null||E===Wr||E===po?V=n.DEPTH24_STENCIL8:E===di?V=n.DEPTH32F_STENCIL8:E===ka&&(V=n.DEPTH24_STENCIL8,console.warn("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):E===null||E===Wr||E===po?V=n.DEPTH_COMPONENT24:E===di?V=n.DEPTH_COMPONENT32F:E===ka&&(V=n.DEPTH_COMPONENT16),V}function A(T,E){return g(T)===!0||T.isFramebufferTexture&&T.minFilter!==_n&&T.minFilter!==vn?Math.log2(Math.max(E.width,E.height))+1:T.mipmaps!==void 0&&T.mipmaps.length>0?T.mipmaps.length:T.isCompressedTexture&&Array.isArray(T.image)?E.mipmaps.length:1}function C(T){let E=T.target;E.removeEventListener("dispose",C),I(E),E.isVideoTexture&&u.delete(E)}function S(T){let E=T.target;E.removeEventListener("dispose",S),x(E)}function I(T){let E=i.get(T);if(E.__webglInit===void 0)return;let V=T.source,Y=h.get(V);if(Y){let K=Y[E.__cacheKey];K.usedTimes--,K.usedTimes===0&&b(T),Object.keys(Y).length===0&&h.delete(V)}i.remove(T)}function b(T){let E=i.get(T);n.deleteTexture(E.__webglTexture);let V=T.source,Y=h.get(V);delete Y[E.__cacheKey],o.memory.textures--}function x(T){let E=i.get(T);if(T.depthTexture&&(T.depthTexture.dispose(),i.remove(T.depthTexture)),T.isWebGLCubeRenderTarget)for(let Y=0;Y<6;Y++){if(Array.isArray(E.__webglFramebuffer[Y]))for(let K=0;K<E.__webglFramebuffer[Y].length;K++)n.deleteFramebuffer(E.__webglFramebuffer[Y][K]);else n.deleteFramebuffer(E.__webglFramebuffer[Y]);E.__webglDepthbuffer&&n.deleteRenderbuffer(E.__webglDepthbuffer[Y])}else{if(Array.isArray(E.__webglFramebuffer))for(let Y=0;Y<E.__webglFramebuffer.length;Y++)n.deleteFramebuffer(E.__webglFramebuffer[Y]);else n.deleteFramebuffer(E.__webglFramebuffer);if(E.__webglDepthbuffer&&n.deleteRenderbuffer(E.__webglDepthbuffer),E.__webglMultisampledFramebuffer&&n.deleteFramebuffer(E.__webglMultisampledFramebuffer),E.__webglColorRenderbuffer)for(let Y=0;Y<E.__webglColorRenderbuffer.length;Y++)E.__webglColorRenderbuffer[Y]&&n.deleteRenderbuffer(E.__webglColorRenderbuffer[Y]);E.__webglDepthRenderbuffer&&n.deleteRenderbuffer(E.__webglDepthRenderbuffer)}let V=T.textures;for(let Y=0,K=V.length;Y<K;Y++){let X=i.get(V[Y]);X.__webglTexture&&(n.deleteTexture(X.__webglTexture),o.memory.textures--),i.remove(V[Y])}i.remove(T)}let D=0;function N(){D=0}function O(){let T=D;return T>=r.maxTextures&&console.warn("THREE.WebGLTextures: Trying to use "+T+" texture units while this GPU supports only "+r.maxTextures),D+=1,T}function U(T){let E=[];return E.push(T.wrapS),E.push(T.wrapT),E.push(T.wrapR||0),E.push(T.magFilter),E.push(T.minFilter),E.push(T.anisotropy),E.push(T.internalFormat),E.push(T.format),E.push(T.type),E.push(T.generateMipmaps),E.push(T.premultiplyAlpha),E.push(T.flipY),E.push(T.unpackAlignment),E.push(T.colorSpace),E.join()}function $(T,E){let V=i.get(T);if(T.isVideoTexture&&Ce(T),T.isRenderTargetTexture===!1&&T.version>0&&V.__version!==T.version){let Y=T.image;if(Y===null)console.warn("THREE.WebGLRenderer: Texture marked for update but no image data found.");else if(Y.complete===!1)console.warn("THREE.WebGLRenderer: Texture marked for update but image is incomplete");else{G(V,T,E);return}}t.bindTexture(n.TEXTURE_2D,V.__webglTexture,n.TEXTURE0+E)}function W(T,E){let V=i.get(T);if(T.version>0&&V.__version!==T.version){G(V,T,E);return}t.bindTexture(n.TEXTURE_2D_ARRAY,V.__webglTexture,n.TEXTURE0+E)}function q(T,E){let V=i.get(T);if(T.version>0&&V.__version!==T.version){G(V,T,E);return}t.bindTexture(n.TEXTURE_3D,V.__webglTexture,n.TEXTURE0+E)}function z(T,E){let V=i.get(T);if(T.version>0&&V.__version!==T.version){J(V,T,E);return}t.bindTexture(n.TEXTURE_CUBE_MAP,V.__webglTexture,n.TEXTURE0+E)}let Q={[lm]:n.REPEAT,[zr]:n.CLAMP_TO_EDGE,[cm]:n.MIRRORED_REPEAT},ee={[_n]:n.NEAREST,[mI]:n.NEAREST_MIPMAP_NEAREST,[Gc]:n.NEAREST_MIPMAP_LINEAR,[vn]:n.LINEAR,[vp]:n.LINEAR_MIPMAP_NEAREST,[Gr]:n.LINEAR_MIPMAP_LINEAR},oe={[_I]:n.NEVER,[SI]:n.ALWAYS,[xI]:n.LESS,[YM]:n.LEQUAL,[MI]:n.EQUAL,[wI]:n.GEQUAL,[EI]:n.GREATER,[bI]:n.NOTEQUAL};function ge(T,E){if(E.type===di&&e.has("OES_texture_float_linear")===!1&&(E.magFilter===vn||E.magFilter===vp||E.magFilter===Gc||E.magFilter===Gr||E.minFilter===vn||E.minFilter===vp||E.minFilter===Gc||E.minFilter===Gr)&&console.warn("THREE.WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(T,n.TEXTURE_WRAP_S,Q[E.wrapS]),n.texParameteri(T,n.TEXTURE_WRAP_T,Q[E.wrapT]),(T===n.TEXTURE_3D||T===n.TEXTURE_2D_ARRAY)&&n.texParameteri(T,n.TEXTURE_WRAP_R,Q[E.wrapR]),n.texParameteri(T,n.TEXTURE_MAG_FILTER,ee[E.magFilter]),n.texParameteri(T,n.TEXTURE_MIN_FILTER,ee[E.minFilter]),E.compareFunction&&(n.texParameteri(T,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(T,n.TEXTURE_COMPARE_FUNC,oe[E.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(E.magFilter===_n||E.minFilter!==Gc&&E.minFilter!==Gr||E.type===di&&e.has("OES_texture_float_linear")===!1)return;if(E.anisotropy>1||i.get(E).__currentAnisotropy){let V=e.get("EXT_texture_filter_anisotropic");n.texParameterf(T,V.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(E.anisotropy,r.getMaxAnisotropy())),i.get(E).__currentAnisotropy=E.anisotropy}}}function we(T,E){let V=!1;T.__webglInit===void 0&&(T.__webglInit=!0,E.addEventListener("dispose",C));let Y=E.source,K=h.get(Y);K===void 0&&(K={},h.set(Y,K));let X=U(E);if(X!==T.__cacheKey){K[X]===void 0&&(K[X]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,V=!0),K[X].usedTimes++;let Me=K[T.__cacheKey];Me!==void 0&&(K[T.__cacheKey].usedTimes--,Me.usedTimes===0&&b(E)),T.__cacheKey=X,T.__webglTexture=K[X].texture}return V}function G(T,E,V){let Y=n.TEXTURE_2D;(E.isDataArrayTexture||E.isCompressedArrayTexture)&&(Y=n.TEXTURE_2D_ARRAY),E.isData3DTexture&&(Y=n.TEXTURE_3D);let K=we(T,E),X=E.source;t.bindTexture(Y,T.__webglTexture,n.TEXTURE0+V);let Me=i.get(X);if(X.version!==Me.__version||K===!0){t.activeTexture(n.TEXTURE0+V);let le=Xe.getPrimaries(Xe.workingColorSpace),he=E.colorSpace===ar?null:Xe.getPrimaries(E.colorSpace),Qe=E.colorSpace===ar||le===he?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,E.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,E.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,E.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Qe);let ne=v(E.image,!1,r.maxTextureSize);ne=xt(E,ne);let pe=s.convert(E.format,E.colorSpace),De=s.convert(E.type),Ae=_(E.internalFormat,pe,De,E.colorSpace,E.isVideoTexture);ge(Y,E);let me,Ke=E.mipmaps,ke=E.isVideoTexture!==!0,gt=Me.__version===void 0||K===!0,P=X.dataReady,ae=A(E,ne);if(E.isDepthTexture)Ae=y(E.format===mo,E.type),gt&&(ke?t.texStorage2D(n.TEXTURE_2D,1,Ae,ne.width,ne.height):t.texImage2D(n.TEXTURE_2D,0,Ae,ne.width,ne.height,0,pe,De,null));else if(E.isDataTexture)if(Ke.length>0){ke&&gt&&t.texStorage2D(n.TEXTURE_2D,ae,Ae,Ke[0].width,Ke[0].height);for(let j=0,Z=Ke.length;j<Z;j++)me=Ke[j],ke?P&&t.texSubImage2D(n.TEXTURE_2D,j,0,0,me.width,me.height,pe,De,me.data):t.texImage2D(n.TEXTURE_2D,j,Ae,me.width,me.height,0,pe,De,me.data);E.generateMipmaps=!1}else ke?(gt&&t.texStorage2D(n.TEXTURE_2D,ae,Ae,ne.width,ne.height),P&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,ne.width,ne.height,pe,De,ne.data)):t.texImage2D(n.TEXTURE_2D,0,Ae,ne.width,ne.height,0,pe,De,ne.data);else if(E.isCompressedTexture)if(E.isCompressedArrayTexture){ke&&gt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ae,Ae,Ke[0].width,Ke[0].height,ne.depth);for(let j=0,Z=Ke.length;j<Z;j++)if(me=Ke[j],E.format!==$n)if(pe!==null)if(ke){if(P)if(E.layerUpdates.size>0){let de=CM(me.width,me.height,E.format,E.type);for(let ce of E.layerUpdates){let Oe=me.data.subarray(ce*de/me.data.BYTES_PER_ELEMENT,(ce+1)*de/me.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,j,0,0,ce,me.width,me.height,1,pe,Oe)}E.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,j,0,0,0,me.width,me.height,ne.depth,pe,me.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,j,Ae,me.width,me.height,ne.depth,0,me.data,0,0);else console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else ke?P&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,j,0,0,0,me.width,me.height,ne.depth,pe,De,me.data):t.texImage3D(n.TEXTURE_2D_ARRAY,j,Ae,me.width,me.height,ne.depth,0,pe,De,me.data)}else{ke&&gt&&t.texStorage2D(n.TEXTURE_2D,ae,Ae,Ke[0].width,Ke[0].height);for(let j=0,Z=Ke.length;j<Z;j++)me=Ke[j],E.format!==$n?pe!==null?ke?P&&t.compressedTexSubImage2D(n.TEXTURE_2D,j,0,0,me.width,me.height,pe,me.data):t.compressedTexImage2D(n.TEXTURE_2D,j,Ae,me.width,me.height,0,me.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):ke?P&&t.texSubImage2D(n.TEXTURE_2D,j,0,0,me.width,me.height,pe,De,me.data):t.texImage2D(n.TEXTURE_2D,j,Ae,me.width,me.height,0,pe,De,me.data)}else if(E.isDataArrayTexture)if(ke){if(gt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ae,Ae,ne.width,ne.height,ne.depth),P)if(E.layerUpdates.size>0){let j=CM(ne.width,ne.height,E.format,E.type);for(let Z of E.layerUpdates){let de=ne.data.subarray(Z*j/ne.data.BYTES_PER_ELEMENT,(Z+1)*j/ne.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,Z,ne.width,ne.height,1,pe,De,de)}E.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,ne.width,ne.height,ne.depth,pe,De,ne.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Ae,ne.width,ne.height,ne.depth,0,pe,De,ne.data);else if(E.isData3DTexture)ke?(gt&&t.texStorage3D(n.TEXTURE_3D,ae,Ae,ne.width,ne.height,ne.depth),P&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,ne.width,ne.height,ne.depth,pe,De,ne.data)):t.texImage3D(n.TEXTURE_3D,0,Ae,ne.width,ne.height,ne.depth,0,pe,De,ne.data);else if(E.isFramebufferTexture){if(gt)if(ke)t.texStorage2D(n.TEXTURE_2D,ae,Ae,ne.width,ne.height);else{let j=ne.width,Z=ne.height;for(let de=0;de<ae;de++)t.texImage2D(n.TEXTURE_2D,de,Ae,j,Z,0,pe,De,null),j>>=1,Z>>=1}}else if(Ke.length>0){if(ke&&gt){let j=Se(Ke[0]);t.texStorage2D(n.TEXTURE_2D,ae,Ae,j.width,j.height)}for(let j=0,Z=Ke.length;j<Z;j++)me=Ke[j],ke?P&&t.texSubImage2D(n.TEXTURE_2D,j,0,0,pe,De,me):t.texImage2D(n.TEXTURE_2D,j,Ae,pe,De,me);E.generateMipmaps=!1}else if(ke){if(gt){let j=Se(ne);t.texStorage2D(n.TEXTURE_2D,ae,Ae,j.width,j.height)}P&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,pe,De,ne)}else t.texImage2D(n.TEXTURE_2D,0,Ae,pe,De,ne);g(E)&&p(Y),Me.__version=X.version,E.onUpdate&&E.onUpdate(E)}T.__version=E.version}function J(T,E,V){if(E.image.length!==6)return;let Y=we(T,E),K=E.source;t.bindTexture(n.TEXTURE_CUBE_MAP,T.__webglTexture,n.TEXTURE0+V);let X=i.get(K);if(K.version!==X.__version||Y===!0){t.activeTexture(n.TEXTURE0+V);let Me=Xe.getPrimaries(Xe.workingColorSpace),le=E.colorSpace===ar?null:Xe.getPrimaries(E.colorSpace),he=E.colorSpace===ar||Me===le?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,E.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,E.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,E.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,he);let Qe=E.isCompressedTexture||E.image[0].isCompressedTexture,ne=E.image[0]&&E.image[0].isDataTexture,pe=[];for(let Z=0;Z<6;Z++)!Qe&&!ne?pe[Z]=v(E.image[Z],!0,r.maxCubemapSize):pe[Z]=ne?E.image[Z].image:E.image[Z],pe[Z]=xt(E,pe[Z]);let De=pe[0],Ae=s.convert(E.format,E.colorSpace),me=s.convert(E.type),Ke=_(E.internalFormat,Ae,me,E.colorSpace),ke=E.isVideoTexture!==!0,gt=X.__version===void 0||Y===!0,P=K.dataReady,ae=A(E,De);ge(n.TEXTURE_CUBE_MAP,E);let j;if(Qe){ke&&gt&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ae,Ke,De.width,De.height);for(let Z=0;Z<6;Z++){j=pe[Z].mipmaps;for(let de=0;de<j.length;de++){let ce=j[de];E.format!==$n?Ae!==null?ke?P&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,de,0,0,ce.width,ce.height,Ae,ce.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,de,Ke,ce.width,ce.height,0,ce.data):console.warn("THREE.WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):ke?P&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,de,0,0,ce.width,ce.height,Ae,me,ce.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,de,Ke,ce.width,ce.height,0,Ae,me,ce.data)}}}else{if(j=E.mipmaps,ke&&gt){j.length>0&&ae++;let Z=Se(pe[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ae,Ke,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(ne){ke?P&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,pe[Z].width,pe[Z].height,Ae,me,pe[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,Ke,pe[Z].width,pe[Z].height,0,Ae,me,pe[Z].data);for(let de=0;de<j.length;de++){let Oe=j[de].image[Z].image;ke?P&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,de+1,0,0,Oe.width,Oe.height,Ae,me,Oe.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,de+1,Ke,Oe.width,Oe.height,0,Ae,me,Oe.data)}}else{ke?P&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,Ae,me,pe[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,Ke,Ae,me,pe[Z]);for(let de=0;de<j.length;de++){let ce=j[de];ke?P&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,de+1,0,0,Ae,me,ce.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,de+1,Ke,Ae,me,ce.image[Z])}}}g(E)&&p(n.TEXTURE_CUBE_MAP),X.__version=K.version,E.onUpdate&&E.onUpdate(E)}T.__version=E.version}function fe(T,E,V,Y,K,X){let Me=s.convert(V.format,V.colorSpace),le=s.convert(V.type),he=_(V.internalFormat,Me,le,V.colorSpace),Qe=i.get(E),ne=i.get(V);if(ne.__renderTarget=E,!Qe.__hasExternalTextures){let pe=Math.max(1,E.width>>X),De=Math.max(1,E.height>>X);K===n.TEXTURE_3D||K===n.TEXTURE_2D_ARRAY?t.texImage3D(K,X,he,pe,De,E.depth,0,Me,le,null):t.texImage2D(K,X,he,pe,De,0,Me,le,null)}t.bindFramebuffer(n.FRAMEBUFFER,T),Ze(E)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Y,K,ne.__webglTexture,0,Ye(E)):(K===n.TEXTURE_2D||K>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&K<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,Y,K,ne.__webglTexture,X),t.bindFramebuffer(n.FRAMEBUFFER,null)}function re(T,E,V){if(n.bindRenderbuffer(n.RENDERBUFFER,T),E.depthBuffer){let Y=E.depthTexture,K=Y&&Y.isDepthTexture?Y.type:null,X=y(E.stencilBuffer,K),Me=E.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,le=Ye(E);Ze(E)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,le,X,E.width,E.height):V?n.renderbufferStorageMultisample(n.RENDERBUFFER,le,X,E.width,E.height):n.renderbufferStorage(n.RENDERBUFFER,X,E.width,E.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,Me,n.RENDERBUFFER,T)}else{let Y=E.textures;for(let K=0;K<Y.length;K++){let X=Y[K],Me=s.convert(X.format,X.colorSpace),le=s.convert(X.type),he=_(X.internalFormat,Me,le,X.colorSpace),Qe=Ye(E);V&&Ze(E)===!1?n.renderbufferStorageMultisample(n.RENDERBUFFER,Qe,he,E.width,E.height):Ze(E)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Qe,he,E.width,E.height):n.renderbufferStorage(n.RENDERBUFFER,he,E.width,E.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function be(T,E){if(E&&E.isWebGLCubeRenderTarget)throw new Error("Depth Texture with cube render targets is not supported");if(t.bindFramebuffer(n.FRAMEBUFFER,T),!(E.depthTexture&&E.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let Y=i.get(E.depthTexture);Y.__renderTarget=E,(!Y.__webglTexture||E.depthTexture.image.width!==E.width||E.depthTexture.image.height!==E.height)&&(E.depthTexture.image.width=E.width,E.depthTexture.image.height=E.height,E.depthTexture.needsUpdate=!0),$(E.depthTexture,0);let K=Y.__webglTexture,X=Ye(E);if(E.depthTexture.format===ao)Ze(E)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,K,0,X):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_ATTACHMENT,n.TEXTURE_2D,K,0);else if(E.depthTexture.format===mo)Ze(E)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,K,0,X):n.framebufferTexture2D(n.FRAMEBUFFER,n.DEPTH_STENCIL_ATTACHMENT,n.TEXTURE_2D,K,0);else throw new Error("Unknown depthTexture format")}function Te(T){let E=i.get(T),V=T.isWebGLCubeRenderTarget===!0;if(E.__boundDepthTexture!==T.depthTexture){let Y=T.depthTexture;if(E.__depthDisposeCallback&&E.__depthDisposeCallback(),Y){let K=()=>{delete E.__boundDepthTexture,delete E.__depthDisposeCallback,Y.removeEventListener("dispose",K)};Y.addEventListener("dispose",K),E.__depthDisposeCallback=K}E.__boundDepthTexture=Y}if(T.depthTexture&&!E.__autoAllocateDepthBuffer){if(V)throw new Error("target.depthTexture not supported in Cube render targets");be(E.__webglFramebuffer,T)}else if(V){E.__webglDepthbuffer=[];for(let Y=0;Y<6;Y++)if(t.bindFramebuffer(n.FRAMEBUFFER,E.__webglFramebuffer[Y]),E.__webglDepthbuffer[Y]===void 0)E.__webglDepthbuffer[Y]=n.createRenderbuffer(),re(E.__webglDepthbuffer[Y],T,!1);else{let K=T.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,X=E.__webglDepthbuffer[Y];n.bindRenderbuffer(n.RENDERBUFFER,X),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,X)}}else if(t.bindFramebuffer(n.FRAMEBUFFER,E.__webglFramebuffer),E.__webglDepthbuffer===void 0)E.__webglDepthbuffer=n.createRenderbuffer(),re(E.__webglDepthbuffer,T,!1);else{let Y=T.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,K=E.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,K),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,K)}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Ne(T,E,V){let Y=i.get(T);E!==void 0&&fe(Y.__webglFramebuffer,T,T.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),V!==void 0&&Te(T)}function nt(T){let E=T.texture,V=i.get(T),Y=i.get(E);T.addEventListener("dispose",S);let K=T.textures,X=T.isWebGLCubeRenderTarget===!0,Me=K.length>1;if(Me||(Y.__webglTexture===void 0&&(Y.__webglTexture=n.createTexture()),Y.__version=E.version,o.memory.textures++),X){V.__webglFramebuffer=[];for(let le=0;le<6;le++)if(E.mipmaps&&E.mipmaps.length>0){V.__webglFramebuffer[le]=[];for(let he=0;he<E.mipmaps.length;he++)V.__webglFramebuffer[le][he]=n.createFramebuffer()}else V.__webglFramebuffer[le]=n.createFramebuffer()}else{if(E.mipmaps&&E.mipmaps.length>0){V.__webglFramebuffer=[];for(let le=0;le<E.mipmaps.length;le++)V.__webglFramebuffer[le]=n.createFramebuffer()}else V.__webglFramebuffer=n.createFramebuffer();if(Me)for(let le=0,he=K.length;le<he;le++){let Qe=i.get(K[le]);Qe.__webglTexture===void 0&&(Qe.__webglTexture=n.createTexture(),o.memory.textures++)}if(T.samples>0&&Ze(T)===!1){V.__webglMultisampledFramebuffer=n.createFramebuffer(),V.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,V.__webglMultisampledFramebuffer);for(let le=0;le<K.length;le++){let he=K[le];V.__webglColorRenderbuffer[le]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,V.__webglColorRenderbuffer[le]);let Qe=s.convert(he.format,he.colorSpace),ne=s.convert(he.type),pe=_(he.internalFormat,Qe,ne,he.colorSpace,T.isXRRenderTarget===!0),De=Ye(T);n.renderbufferStorageMultisample(n.RENDERBUFFER,De,pe,T.width,T.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+le,n.RENDERBUFFER,V.__webglColorRenderbuffer[le])}n.bindRenderbuffer(n.RENDERBUFFER,null),T.depthBuffer&&(V.__webglDepthRenderbuffer=n.createRenderbuffer(),re(V.__webglDepthRenderbuffer,T,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(X){t.bindTexture(n.TEXTURE_CUBE_MAP,Y.__webglTexture),ge(n.TEXTURE_CUBE_MAP,E);for(let le=0;le<6;le++)if(E.mipmaps&&E.mipmaps.length>0)for(let he=0;he<E.mipmaps.length;he++)fe(V.__webglFramebuffer[le][he],T,E,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+le,he);else fe(V.__webglFramebuffer[le],T,E,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+le,0);g(E)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(Me){for(let le=0,he=K.length;le<he;le++){let Qe=K[le],ne=i.get(Qe);t.bindTexture(n.TEXTURE_2D,ne.__webglTexture),ge(n.TEXTURE_2D,Qe),fe(V.__webglFramebuffer,T,Qe,n.COLOR_ATTACHMENT0+le,n.TEXTURE_2D,0),g(Qe)&&p(n.TEXTURE_2D)}t.unbindTexture()}else{let le=n.TEXTURE_2D;if((T.isWebGL3DRenderTarget||T.isWebGLArrayRenderTarget)&&(le=T.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(le,Y.__webglTexture),ge(le,E),E.mipmaps&&E.mipmaps.length>0)for(let he=0;he<E.mipmaps.length;he++)fe(V.__webglFramebuffer[he],T,E,n.COLOR_ATTACHMENT0,le,he);else fe(V.__webglFramebuffer,T,E,n.COLOR_ATTACHMENT0,le,0);g(E)&&p(le),t.unbindTexture()}T.depthBuffer&&Te(T)}function ze(T){let E=T.textures;for(let V=0,Y=E.length;V<Y;V++){let K=E[V];if(g(K)){let X=M(T),Me=i.get(K).__webglTexture;t.bindTexture(X,Me),p(X),t.unbindTexture()}}}let _t=[],k=[];function wn(T){if(T.samples>0){if(Ze(T)===!1){let E=T.textures,V=T.width,Y=T.height,K=n.COLOR_BUFFER_BIT,X=T.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,Me=i.get(T),le=E.length>1;if(le)for(let he=0;he<E.length;he++)t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,Me.__webglMultisampledFramebuffer),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglFramebuffer);for(let he=0;he<E.length;he++){if(T.resolveDepthBuffer&&(T.depthBuffer&&(K|=n.DEPTH_BUFFER_BIT),T.stencilBuffer&&T.resolveStencilBuffer&&(K|=n.STENCIL_BUFFER_BIT)),le){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,Me.__webglColorRenderbuffer[he]);let Qe=i.get(E[he]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,Qe,0)}n.blitFramebuffer(0,0,V,Y,0,0,V,Y,K,n.NEAREST),l===!0&&(_t.length=0,k.length=0,_t.push(n.COLOR_ATTACHMENT0+he),T.depthBuffer&&T.resolveDepthBuffer===!1&&(_t.push(X),k.push(X),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,k)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,_t))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),le)for(let he=0;he<E.length;he++){t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,Me.__webglColorRenderbuffer[he]);let Qe=i.get(E[he]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,Me.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,Qe,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,Me.__webglMultisampledFramebuffer)}else if(T.depthBuffer&&T.resolveDepthBuffer===!1&&l){let E=T.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[E])}}}function Ye(T){return Math.min(r.maxSamples,T.samples)}function Ze(T){let E=i.get(T);return T.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&E.__useRenderToTexture!==!1}function Ce(T){let E=o.render.frame;u.get(T)!==E&&(u.set(T,E),T.update())}function xt(T,E){let V=T.colorSpace,Y=T.format,K=T.type;return T.isCompressedTexture===!0||T.isVideoTexture===!0||V!==So&&V!==ar&&(Xe.getTransfer(V)===ot?(Y!==$n||K!==Ui)&&console.warn("THREE.WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):console.error("THREE.WebGLTextures: Unsupported texture color space:",V)),E}function Se(T){return typeof HTMLImageElement<"u"&&T instanceof HTMLImageElement?(c.width=T.naturalWidth||T.width,c.height=T.naturalHeight||T.height):typeof VideoFrame<"u"&&T instanceof VideoFrame?(c.width=T.displayWidth,c.height=T.displayHeight):(c.width=T.width,c.height=T.height),c}this.allocateTextureUnit=O,this.resetTextureUnits=N,this.setTexture2D=$,this.setTexture2DArray=W,this.setTexture3D=q,this.setTextureCube=z,this.rebindTextures=Ne,this.setupRenderTarget=nt,this.updateRenderTargetMipmap=ze,this.updateMultisampleRenderTarget=wn,this.setupDepthRenderbuffer=Te,this.setupFrameBufferTexture=fe,this.useMultisampledRTT=Ze}function RO(n,e){function t(i,r=ar){let s,o=Xe.getTransfer(r);if(i===Ui)return n.UNSIGNED_BYTE;if(i===Dg)return n.UNSIGNED_SHORT_4_4_4_4;if(i===Ag)return n.UNSIGNED_SHORT_5_5_5_1;if(i===zM)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===BM)return n.BYTE;if(i===HM)return n.SHORT;if(i===ka)return n.UNSIGNED_SHORT;if(i===Tg)return n.INT;if(i===Wr)return n.UNSIGNED_INT;if(i===di)return n.FLOAT;if(i===ns)return n.HALF_FLOAT;if(i===GM)return n.ALPHA;if(i===WM)return n.RGB;if(i===$n)return n.RGBA;if(i===jM)return n.LUMINANCE;if(i===$M)return n.LUMINANCE_ALPHA;if(i===ao)return n.DEPTH_COMPONENT;if(i===mo)return n.DEPTH_STENCIL;if(i===Ig)return n.RED;if(i===Rg)return n.RED_INTEGER;if(i===qM)return n.RG;if(i===Ng)return n.RG_INTEGER;if(i===Pg)return n.RGBA_INTEGER;if(i===vu||i===_u||i===xu||i===Mu)if(o===ot)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===vu)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===_u)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===xu)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===Mu)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===vu)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===_u)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===xu)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===Mu)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===um||i===dm||i===fm||i===hm)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===um)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===dm)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===fm)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===hm)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===pm||i===mm||i===gm)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===pm||i===mm)return o===ot?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===gm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC}else return null;if(i===ym||i===vm||i===_m||i===xm||i===Mm||i===Em||i===bm||i===wm||i===Sm||i===Cm||i===Tm||i===Dm||i===Am||i===Im)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===ym)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===vm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===_m)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===xm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===Mm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Em)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===bm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===wm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Sm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Cm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Tm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Dm)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===Am)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Im)return o===ot?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Eu||i===Rm||i===Nm)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Eu)return o===ot?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Rm)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Nm)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===XM||i===Pm||i===Om||i===Lm)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Eu)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Pm)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===Om)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===Lm)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===po?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var Km=class extends sn{constructor(e=[]){super(),this.isArrayCamera=!0,this.cameras=e}},fi=class extends qn{constructor(){super(),this.isGroup=!0,this.type="Group"}},NO={type:"move"},Pa=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new fi,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new fi,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new R,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new R),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new fi,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new R,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new R),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,l=this._grip,c=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(c&&e.hand){o=!0;for(let v of e.hand.values()){let g=t.getJointPose(v,i),p=this._getHandJoint(c,v);g!==null&&(p.matrix.fromArray(g.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=g.radius),p.visible=g!==null}let u=c.joints["index-finger-tip"],d=c.joints["thumb-tip"],h=u.position.distanceTo(d.position),f=.02,m=.005;c.inputState.pinching&&h>f+m?(c.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!c.inputState.pinching&&h<=f-m&&(c.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else l!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(l.matrix.fromArray(s.transform.matrix),l.matrix.decompose(l.position,l.rotation,l.scale),l.matrixWorldNeedsUpdate=!0,s.linearVelocity?(l.hasLinearVelocity=!0,l.linearVelocity.copy(s.linearVelocity)):l.hasLinearVelocity=!1,s.angularVelocity?(l.hasAngularVelocity=!0,l.angularVelocity.copy(s.angularVelocity)):l.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(NO)))}return a!==null&&(a.visible=r!==null),l!==null&&(l.visible=s!==null),c!==null&&(c.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new fi;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},PO=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,OO=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,Jm=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t,i){if(this.texture===null){let r=new zi,s=e.properties.get(r);s.__webglTexture=t.texture,(t.depthNear!=i.depthNear||t.depthFar!=i.depthFar)&&(this.depthNear=t.depthNear,this.depthFar=t.depthFar),this.texture=r}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new Gt({vertexShader:PO,fragmentShader:OO,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new wt(new Xr(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},Qm=class extends Vi{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",l=1,c=null,u=null,d=null,h=null,f=null,m=null,v=new Jm,g=t.getContextAttributes(),p=null,M=null,_=[],y=[],A=new te,C=null,S=new sn;S.viewport=new At;let I=new sn;I.viewport=new At;let b=[S,I],x=new Km,D=null,N=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(G){let J=_[G];return J===void 0&&(J=new Pa,_[G]=J),J.getTargetRaySpace()},this.getControllerGrip=function(G){let J=_[G];return J===void 0&&(J=new Pa,_[G]=J),J.getGripSpace()},this.getHand=function(G){let J=_[G];return J===void 0&&(J=new Pa,_[G]=J),J.getHandSpace()};function O(G){let J=y.indexOf(G.inputSource);if(J===-1)return;let fe=_[J];fe!==void 0&&(fe.update(G.inputSource,G.frame,c||o),fe.dispatchEvent({type:G.type,data:G.inputSource}))}function U(){r.removeEventListener("select",O),r.removeEventListener("selectstart",O),r.removeEventListener("selectend",O),r.removeEventListener("squeeze",O),r.removeEventListener("squeezestart",O),r.removeEventListener("squeezeend",O),r.removeEventListener("end",U),r.removeEventListener("inputsourceschange",$);for(let G=0;G<_.length;G++){let J=y[G];J!==null&&(y[G]=null,_[G].disconnect(J))}D=null,N=null,v.reset(),e.setRenderTarget(p),f=null,h=null,d=null,r=null,M=null,we.stop(),i.isPresenting=!1,e.setPixelRatio(C),e.setSize(A.width,A.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(G){s=G,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(G){a=G,i.isPresenting===!0&&console.warn("THREE.WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return c||o},this.setReferenceSpace=function(G){c=G},this.getBaseLayer=function(){return h!==null?h:f},this.getBinding=function(){return d},this.getFrame=function(){return m},this.getSession=function(){return r},this.setSession=function(G){return jt(this,null,function*(){if(r=G,r!==null){if(p=e.getRenderTarget(),r.addEventListener("select",O),r.addEventListener("selectstart",O),r.addEventListener("selectend",O),r.addEventListener("squeeze",O),r.addEventListener("squeezestart",O),r.addEventListener("squeezeend",O),r.addEventListener("end",U),r.addEventListener("inputsourceschange",$),g.xrCompatible!==!0&&(yield t.makeXRCompatible()),C=e.getPixelRatio(),e.getSize(A),r.renderState.layers===void 0){let J={antialias:g.antialias,alpha:!0,depth:g.depth,stencil:g.stencil,framebufferScaleFactor:s};f=new XRWebGLLayer(r,t,J),r.updateRenderState({baseLayer:f}),e.setPixelRatio(1),e.setSize(f.framebufferWidth,f.framebufferHeight,!1),M=new xn(f.framebufferWidth,f.framebufferHeight,{format:$n,type:Ui,colorSpace:e.outputColorSpace,stencilBuffer:g.stencil})}else{let J=null,fe=null,re=null;g.depth&&(re=g.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,J=g.stencil?mo:ao,fe=g.stencil?po:Wr);let be={colorFormat:t.RGBA8,depthFormat:re,scaleFactor:s};d=new XRWebGLBinding(r,t),h=d.createProjectionLayer(be),r.updateRenderState({layers:[h]}),e.setPixelRatio(1),e.setSize(h.textureWidth,h.textureHeight,!1),M=new xn(h.textureWidth,h.textureHeight,{format:$n,type:Ui,depthTexture:new Yr(h.textureWidth,h.textureHeight,fe,void 0,void 0,void 0,void 0,void 0,void 0,J),stencilBuffer:g.stencil,colorSpace:e.outputColorSpace,samples:g.antialias?4:0,resolveDepthBuffer:h.ignoreDepthValues===!1})}M.isXRRenderTarget=!0,this.setFoveation(l),c=null,o=yield r.requestReferenceSpace(a),we.setContext(r),we.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return v.getDepthTexture()};function $(G){for(let J=0;J<G.removed.length;J++){let fe=G.removed[J],re=y.indexOf(fe);re>=0&&(y[re]=null,_[re].disconnect(fe))}for(let J=0;J<G.added.length;J++){let fe=G.added[J],re=y.indexOf(fe);if(re===-1){for(let Te=0;Te<_.length;Te++)if(Te>=y.length){y.push(fe),re=Te;break}else if(y[Te]===null){y[Te]=fe,re=Te;break}if(re===-1)break}let be=_[re];be&&be.connect(fe)}}let W=new R,q=new R;function z(G,J,fe){W.setFromMatrixPosition(J.matrixWorld),q.setFromMatrixPosition(fe.matrixWorld);let re=W.distanceTo(q),be=J.projectionMatrix.elements,Te=fe.projectionMatrix.elements,Ne=be[14]/(be[10]-1),nt=be[14]/(be[10]+1),ze=(be[9]+1)/be[5],_t=(be[9]-1)/be[5],k=(be[8]-1)/be[0],wn=(Te[8]+1)/Te[0],Ye=Ne*k,Ze=Ne*wn,Ce=re/(-k+wn),xt=Ce*-k;if(J.matrixWorld.decompose(G.position,G.quaternion,G.scale),G.translateX(xt),G.translateZ(Ce),G.matrixWorld.compose(G.position,G.quaternion,G.scale),G.matrixWorldInverse.copy(G.matrixWorld).invert(),be[10]===-1)G.projectionMatrix.copy(J.projectionMatrix),G.projectionMatrixInverse.copy(J.projectionMatrixInverse);else{let Se=Ne+Ce,T=nt+Ce,E=Ye-xt,V=Ze+(re-xt),Y=ze*nt/T*Se,K=_t*nt/T*Se;G.projectionMatrix.makePerspective(E,V,Y,K,Se,T),G.projectionMatrixInverse.copy(G.projectionMatrix).invert()}}function Q(G,J){J===null?G.matrixWorld.copy(G.matrix):G.matrixWorld.multiplyMatrices(J.matrixWorld,G.matrix),G.matrixWorldInverse.copy(G.matrixWorld).invert()}this.updateCamera=function(G){if(r===null)return;let J=G.near,fe=G.far;v.texture!==null&&(v.depthNear>0&&(J=v.depthNear),v.depthFar>0&&(fe=v.depthFar)),x.near=I.near=S.near=J,x.far=I.far=S.far=fe,(D!==x.near||N!==x.far)&&(r.updateRenderState({depthNear:x.near,depthFar:x.far}),D=x.near,N=x.far),S.layers.mask=G.layers.mask|2,I.layers.mask=G.layers.mask|4,x.layers.mask=S.layers.mask|I.layers.mask;let re=G.parent,be=x.cameras;Q(x,re);for(let Te=0;Te<be.length;Te++)Q(be[Te],re);be.length===2?z(x,S,I):x.projectionMatrix.copy(S.projectionMatrix),ee(G,x,re)};function ee(G,J,fe){fe===null?G.matrix.copy(J.matrixWorld):(G.matrix.copy(fe.matrixWorld),G.matrix.invert(),G.matrix.multiply(J.matrixWorld)),G.matrix.decompose(G.position,G.quaternion,G.scale),G.updateMatrixWorld(!0),G.projectionMatrix.copy(J.projectionMatrix),G.projectionMatrixInverse.copy(J.projectionMatrixInverse),G.isPerspectiveCamera&&(G.fov=Ua*2*Math.atan(1/G.projectionMatrix.elements[5]),G.zoom=1)}this.getCamera=function(){return x},this.getFoveation=function(){if(!(h===null&&f===null))return l},this.setFoveation=function(G){l=G,h!==null&&(h.fixedFoveation=G),f!==null&&f.fixedFoveation!==void 0&&(f.fixedFoveation=G)},this.hasDepthSensing=function(){return v.texture!==null},this.getDepthSensingMesh=function(){return v.getMesh(x)};let oe=null;function ge(G,J){if(u=J.getViewerPose(c||o),m=J,u!==null){let fe=u.views;f!==null&&(e.setRenderTargetFramebuffer(M,f.framebuffer),e.setRenderTarget(M));let re=!1;fe.length!==x.cameras.length&&(x.cameras.length=0,re=!0);for(let Te=0;Te<fe.length;Te++){let Ne=fe[Te],nt=null;if(f!==null)nt=f.getViewport(Ne);else{let _t=d.getViewSubImage(h,Ne);nt=_t.viewport,Te===0&&(e.setRenderTargetTextures(M,_t.colorTexture,h.ignoreDepthValues?void 0:_t.depthStencilTexture),e.setRenderTarget(M))}let ze=b[Te];ze===void 0&&(ze=new sn,ze.layers.enable(Te),ze.viewport=new At,b[Te]=ze),ze.matrix.fromArray(Ne.transform.matrix),ze.matrix.decompose(ze.position,ze.quaternion,ze.scale),ze.projectionMatrix.fromArray(Ne.projectionMatrix),ze.projectionMatrixInverse.copy(ze.projectionMatrix).invert(),ze.viewport.set(nt.x,nt.y,nt.width,nt.height),Te===0&&(x.matrix.copy(ze.matrix),x.matrix.decompose(x.position,x.quaternion,x.scale)),re===!0&&x.cameras.push(ze)}let be=r.enabledFeatures;if(be&&be.includes("depth-sensing")){let Te=d.getDepthInformation(fe[0]);Te&&Te.isValid&&Te.texture&&v.init(e,Te,r.renderState)}}for(let fe=0;fe<_.length;fe++){let re=y[fe],be=_[fe];re!==null&&be!==void 0&&be.update(re,J,c||o)}oe&&oe(G,J),J.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:J}),m=null}let we=new tE;we.setAnimationLoop(ge),this.setAnimationLoop=function(G){oe=G},this.dispose=function(){}}},Ur=new $r,LO=new tt;function FO(n,e){function t(g,p){g.matrixAutoUpdate===!0&&g.updateMatrix(),p.value.copy(g.matrix)}function i(g,p){p.color.getRGB(g.fogColor.value,eE(n)),p.isFog?(g.fogNear.value=p.near,g.fogFar.value=p.far):p.isFogExp2&&(g.fogDensity.value=p.density)}function r(g,p,M,_,y){p.isMeshBasicMaterial||p.isMeshLambertMaterial?s(g,p):p.isMeshToonMaterial?(s(g,p),d(g,p)):p.isMeshPhongMaterial?(s(g,p),u(g,p)):p.isMeshStandardMaterial?(s(g,p),h(g,p),p.isMeshPhysicalMaterial&&f(g,p,y)):p.isMeshMatcapMaterial?(s(g,p),m(g,p)):p.isMeshDepthMaterial?s(g,p):p.isMeshDistanceMaterial?(s(g,p),v(g,p)):p.isMeshNormalMaterial?s(g,p):p.isLineBasicMaterial?(o(g,p),p.isLineDashedMaterial&&a(g,p)):p.isPointsMaterial?l(g,p,M,_):p.isSpriteMaterial?c(g,p):p.isShadowMaterial?(g.color.value.copy(p.color),g.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(g,p){g.opacity.value=p.opacity,p.color&&g.diffuse.value.copy(p.color),p.emissive&&g.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(g.map.value=p.map,t(p.map,g.mapTransform)),p.alphaMap&&(g.alphaMap.value=p.alphaMap,t(p.alphaMap,g.alphaMapTransform)),p.bumpMap&&(g.bumpMap.value=p.bumpMap,t(p.bumpMap,g.bumpMapTransform),g.bumpScale.value=p.bumpScale,p.side===Yt&&(g.bumpScale.value*=-1)),p.normalMap&&(g.normalMap.value=p.normalMap,t(p.normalMap,g.normalMapTransform),g.normalScale.value.copy(p.normalScale),p.side===Yt&&g.normalScale.value.negate()),p.displacementMap&&(g.displacementMap.value=p.displacementMap,t(p.displacementMap,g.displacementMapTransform),g.displacementScale.value=p.displacementScale,g.displacementBias.value=p.displacementBias),p.emissiveMap&&(g.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,g.emissiveMapTransform)),p.specularMap&&(g.specularMap.value=p.specularMap,t(p.specularMap,g.specularMapTransform)),p.alphaTest>0&&(g.alphaTest.value=p.alphaTest);let M=e.get(p),_=M.envMap,y=M.envMapRotation;_&&(g.envMap.value=_,Ur.copy(y),Ur.x*=-1,Ur.y*=-1,Ur.z*=-1,_.isCubeTexture&&_.isRenderTargetTexture===!1&&(Ur.y*=-1,Ur.z*=-1),g.envMapRotation.value.setFromMatrix4(LO.makeRotationFromEuler(Ur)),g.flipEnvMap.value=_.isCubeTexture&&_.isRenderTargetTexture===!1?-1:1,g.reflectivity.value=p.reflectivity,g.ior.value=p.ior,g.refractionRatio.value=p.refractionRatio),p.lightMap&&(g.lightMap.value=p.lightMap,g.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,g.lightMapTransform)),p.aoMap&&(g.aoMap.value=p.aoMap,g.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,g.aoMapTransform))}function o(g,p){g.diffuse.value.copy(p.color),g.opacity.value=p.opacity,p.map&&(g.map.value=p.map,t(p.map,g.mapTransform))}function a(g,p){g.dashSize.value=p.dashSize,g.totalSize.value=p.dashSize+p.gapSize,g.scale.value=p.scale}function l(g,p,M,_){g.diffuse.value.copy(p.color),g.opacity.value=p.opacity,g.size.value=p.size*M,g.scale.value=_*.5,p.map&&(g.map.value=p.map,t(p.map,g.uvTransform)),p.alphaMap&&(g.alphaMap.value=p.alphaMap,t(p.alphaMap,g.alphaMapTransform)),p.alphaTest>0&&(g.alphaTest.value=p.alphaTest)}function c(g,p){g.diffuse.value.copy(p.color),g.opacity.value=p.opacity,g.rotation.value=p.rotation,p.map&&(g.map.value=p.map,t(p.map,g.mapTransform)),p.alphaMap&&(g.alphaMap.value=p.alphaMap,t(p.alphaMap,g.alphaMapTransform)),p.alphaTest>0&&(g.alphaTest.value=p.alphaTest)}function u(g,p){g.specular.value.copy(p.specular),g.shininess.value=Math.max(p.shininess,1e-4)}function d(g,p){p.gradientMap&&(g.gradientMap.value=p.gradientMap)}function h(g,p){g.metalness.value=p.metalness,p.metalnessMap&&(g.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,g.metalnessMapTransform)),g.roughness.value=p.roughness,p.roughnessMap&&(g.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,g.roughnessMapTransform)),p.envMap&&(g.envMapIntensity.value=p.envMapIntensity)}function f(g,p,M){g.ior.value=p.ior,p.sheen>0&&(g.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),g.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(g.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,g.sheenColorMapTransform)),p.sheenRoughnessMap&&(g.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,g.sheenRoughnessMapTransform))),p.clearcoat>0&&(g.clearcoat.value=p.clearcoat,g.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(g.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,g.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(g.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,g.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(g.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,g.clearcoatNormalMapTransform),g.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===Yt&&g.clearcoatNormalScale.value.negate())),p.dispersion>0&&(g.dispersion.value=p.dispersion),p.iridescence>0&&(g.iridescence.value=p.iridescence,g.iridescenceIOR.value=p.iridescenceIOR,g.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],g.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(g.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,g.iridescenceMapTransform)),p.iridescenceThicknessMap&&(g.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,g.iridescenceThicknessMapTransform))),p.transmission>0&&(g.transmission.value=p.transmission,g.transmissionSamplerMap.value=M.texture,g.transmissionSamplerSize.value.set(M.width,M.height),p.transmissionMap&&(g.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,g.transmissionMapTransform)),g.thickness.value=p.thickness,p.thicknessMap&&(g.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,g.thicknessMapTransform)),g.attenuationDistance.value=p.attenuationDistance,g.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(g.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(g.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,g.anisotropyMapTransform))),g.specularIntensity.value=p.specularIntensity,g.specularColor.value.copy(p.specularColor),p.specularColorMap&&(g.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,g.specularColorMapTransform)),p.specularIntensityMap&&(g.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,g.specularIntensityMapTransform))}function m(g,p){p.matcap&&(g.matcap.value=p.matcap)}function v(g,p){let M=e.get(p).light;g.referencePosition.value.setFromMatrixPosition(M.matrixWorld),g.nearDistance.value=M.shadow.camera.near,g.farDistance.value=M.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function kO(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function l(M,_){let y=_.program;i.uniformBlockBinding(M,y)}function c(M,_){let y=r[M.id];y===void 0&&(m(M),y=u(M),r[M.id]=y,M.addEventListener("dispose",g));let A=_.program;i.updateUBOMapping(M,A);let C=e.render.frame;s[M.id]!==C&&(h(M),s[M.id]=C)}function u(M){let _=d();M.__bindingPointIndex=_;let y=n.createBuffer(),A=M.__size,C=M.usage;return n.bindBuffer(n.UNIFORM_BUFFER,y),n.bufferData(n.UNIFORM_BUFFER,A,C),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,_,y),y}function d(){for(let M=0;M<a;M++)if(o.indexOf(M)===-1)return o.push(M),M;return console.error("THREE.WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function h(M){let _=r[M.id],y=M.uniforms,A=M.__cache;n.bindBuffer(n.UNIFORM_BUFFER,_);for(let C=0,S=y.length;C<S;C++){let I=Array.isArray(y[C])?y[C]:[y[C]];for(let b=0,x=I.length;b<x;b++){let D=I[b];if(f(D,C,b,A)===!0){let N=D.__offset,O=Array.isArray(D.value)?D.value:[D.value],U=0;for(let $=0;$<O.length;$++){let W=O[$],q=v(W);typeof W=="number"||typeof W=="boolean"?(D.__data[0]=W,n.bufferSubData(n.UNIFORM_BUFFER,N+U,D.__data)):W.isMatrix3?(D.__data[0]=W.elements[0],D.__data[1]=W.elements[1],D.__data[2]=W.elements[2],D.__data[3]=0,D.__data[4]=W.elements[3],D.__data[5]=W.elements[4],D.__data[6]=W.elements[5],D.__data[7]=0,D.__data[8]=W.elements[6],D.__data[9]=W.elements[7],D.__data[10]=W.elements[8],D.__data[11]=0):(W.toArray(D.__data,U),U+=q.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,N,D.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function f(M,_,y,A){let C=M.value,S=_+"_"+y;if(A[S]===void 0)return typeof C=="number"||typeof C=="boolean"?A[S]=C:A[S]=C.clone(),!0;{let I=A[S];if(typeof C=="number"||typeof C=="boolean"){if(I!==C)return A[S]=C,!0}else if(I.equals(C)===!1)return I.copy(C),!0}return!1}function m(M){let _=M.uniforms,y=0,A=16;for(let S=0,I=_.length;S<I;S++){let b=Array.isArray(_[S])?_[S]:[_[S]];for(let x=0,D=b.length;x<D;x++){let N=b[x],O=Array.isArray(N.value)?N.value:[N.value];for(let U=0,$=O.length;U<$;U++){let W=O[U],q=v(W),z=y%A,Q=z%q.boundary,ee=z+Q;y+=Q,ee!==0&&A-ee<q.storage&&(y+=A-ee),N.__data=new Float32Array(q.storage/Float32Array.BYTES_PER_ELEMENT),N.__offset=y,y+=q.storage}}}let C=y%A;return C>0&&(y+=A-C),M.__size=y,M.__cache={},this}function v(M){let _={boundary:0,storage:0};return typeof M=="number"||typeof M=="boolean"?(_.boundary=4,_.storage=4):M.isVector2?(_.boundary=8,_.storage=8):M.isVector3||M.isColor?(_.boundary=16,_.storage=12):M.isVector4?(_.boundary=16,_.storage=16):M.isMatrix3?(_.boundary=48,_.storage=48):M.isMatrix4?(_.boundary=64,_.storage=64):M.isTexture?console.warn("THREE.WebGLRenderer: Texture samplers can not be part of an uniforms group."):console.warn("THREE.WebGLRenderer: Unsupported uniform value type.",M),_}function g(M){let _=M.target;_.removeEventListener("dispose",g);let y=o.indexOf(_.__bindingPointIndex);o.splice(y,1),n.deleteBuffer(r[_.id]),delete r[_.id],delete s[_.id]}function p(){for(let M in r)n.deleteBuffer(r[M]);o=[],r={},s={}}return{bind:l,update:c,dispose:p}}var Ou=class{constructor(e={}){let{canvas:t=zI(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:l=!0,preserveDrawingBuffer:c=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reverseDepthBuffer:h=!1}=e;this.isWebGLRenderer=!0;let f;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");f=i.getContextAttributes().alpha}else f=o;let m=new Uint32Array(4),v=new Int32Array(4),g=null,p=null,M=[],_=[];this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this._outputColorSpace=rn,this.toneMapping=pi,this.toneMappingExposure=1;let y=this,A=!1,C=0,S=0,I=null,b=-1,x=null,D=new At,N=new At,O=null,U=new xe(0),$=0,W=t.width,q=t.height,z=1,Q=null,ee=null,oe=new At(0,0,W,q),ge=new At(0,0,W,q),we=!1,G=new Ba,J=!1,fe=!1,re=new tt,be=new tt,Te=new R,Ne=new At,nt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},ze=!1;function _t(){return I===null?z:1}let k=i;function wn(w,L){return t.getContext(w,L)}try{let w={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:l,preserveDrawingBuffer:c,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine","three.js r170"),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",de,!1),t.addEventListener("webglcontextcreationerror",ce,!1),k===null){let L="webgl2";if(k=wn(L,w),k===null)throw wn(L)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(w){throw console.error("THREE.WebGLRenderer: "+w.message),w}let Ye,Ze,Ce,xt,Se,T,E,V,Y,K,X,Me,le,he,Qe,ne,pe,De,Ae,me,Ke,ke,gt,P;function ae(){Ye=new QN(k),Ye.init(),ke=new RO(k,Ye),Ze=new qN(k,Ye,e,ke),Ce=new DO(k,Ye),Ze.reverseDepthBuffer&&h&&Ce.buffers.depth.setReversed(!0),xt=new nP(k),Se=new mO,T=new IO(k,Ye,Ce,Se,Ze,ke,xt),E=new YN(y),V=new JN(y),Y=new c1(k),gt=new jN(k,Y),K=new eP(k,Y,xt,gt),X=new rP(k,K,Y,xt),Ae=new iP(k,Ze,T),ne=new XN(Se),Me=new pO(y,E,V,Ye,Ze,gt,ne),le=new FO(y,Se),he=new yO,Qe=new bO(Ye),De=new WN(y,E,V,Ce,X,f,l),pe=new CO(y,X,Ze),P=new kO(k,xt,Ze,Ce),me=new $N(k,Ye,xt),Ke=new tP(k,Ye,xt),xt.programs=Me.programs,y.capabilities=Ze,y.extensions=Ye,y.properties=Se,y.renderLists=he,y.shadowMap=pe,y.state=Ce,y.info=xt}ae();let j=new Qm(y,k);this.xr=j,this.getContext=function(){return k},this.getContextAttributes=function(){return k.getContextAttributes()},this.forceContextLoss=function(){let w=Ye.get("WEBGL_lose_context");w&&w.loseContext()},this.forceContextRestore=function(){let w=Ye.get("WEBGL_lose_context");w&&w.restoreContext()},this.getPixelRatio=function(){return z},this.setPixelRatio=function(w){w!==void 0&&(z=w,this.setSize(W,q,!1))},this.getSize=function(w){return w.set(W,q)},this.setSize=function(w,L,B=!0){if(j.isPresenting){console.warn("THREE.WebGLRenderer: Can't change size while VR device is presenting.");return}W=w,q=L,t.width=Math.floor(w*z),t.height=Math.floor(L*z),B===!0&&(t.style.width=w+"px",t.style.height=L+"px"),this.setViewport(0,0,w,L)},this.getDrawingBufferSize=function(w){return w.set(W*z,q*z).floor()},this.setDrawingBufferSize=function(w,L,B){W=w,q=L,z=B,t.width=Math.floor(w*B),t.height=Math.floor(L*B),this.setViewport(0,0,w,L)},this.getCurrentViewport=function(w){return w.copy(D)},this.getViewport=function(w){return w.copy(oe)},this.setViewport=function(w,L,B,H){w.isVector4?oe.set(w.x,w.y,w.z,w.w):oe.set(w,L,B,H),Ce.viewport(D.copy(oe).multiplyScalar(z).round())},this.getScissor=function(w){return w.copy(ge)},this.setScissor=function(w,L,B,H){w.isVector4?ge.set(w.x,w.y,w.z,w.w):ge.set(w,L,B,H),Ce.scissor(N.copy(ge).multiplyScalar(z).round())},this.getScissorTest=function(){return we},this.setScissorTest=function(w){Ce.setScissorTest(we=w)},this.setOpaqueSort=function(w){Q=w},this.setTransparentSort=function(w){ee=w},this.getClearColor=function(w){return w.copy(De.getClearColor())},this.setClearColor=function(){De.setClearColor.apply(De,arguments)},this.getClearAlpha=function(){return De.getClearAlpha()},this.setClearAlpha=function(){De.setClearAlpha.apply(De,arguments)},this.clear=function(w=!0,L=!0,B=!0){let H=0;if(w){let F=!1;if(I!==null){let ie=I.texture.format;F=ie===Pg||ie===Ng||ie===Rg}if(F){let ie=I.texture.type,ue=ie===Ui||ie===Wr||ie===ka||ie===po||ie===Dg||ie===Ag,ye=De.getClearColor(),ve=De.getClearAlpha(),Ie=ye.r,Le=ye.g,_e=ye.b;ue?(m[0]=Ie,m[1]=Le,m[2]=_e,m[3]=ve,k.clearBufferuiv(k.COLOR,0,m)):(v[0]=Ie,v[1]=Le,v[2]=_e,v[3]=ve,k.clearBufferiv(k.COLOR,0,v))}else H|=k.COLOR_BUFFER_BIT}L&&(H|=k.DEPTH_BUFFER_BIT),B&&(H|=k.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),k.clear(H)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",de,!1),t.removeEventListener("webglcontextcreationerror",ce,!1),he.dispose(),Qe.dispose(),Se.dispose(),E.dispose(),V.dispose(),X.dispose(),gt.dispose(),P.dispose(),Me.dispose(),j.dispose(),j.removeEventListener("sessionstart",Yg),j.removeEventListener("sessionend",Zg),dr.stop()};function Z(w){w.preventDefault(),console.log("THREE.WebGLRenderer: Context Lost."),A=!0}function de(){console.log("THREE.WebGLRenderer: Context Restored."),A=!1;let w=xt.autoReset,L=pe.enabled,B=pe.autoUpdate,H=pe.needsUpdate,F=pe.type;ae(),xt.autoReset=w,pe.enabled=L,pe.autoUpdate=B,pe.needsUpdate=H,pe.type=F}function ce(w){console.error("THREE.WebGLRenderer: A WebGL context could not be created. Reason: ",w.statusMessage)}function Oe(w){let L=w.target;L.removeEventListener("dispose",Oe),Tt(L)}function Tt(w){Wt(w),Se.remove(w)}function Wt(w){let L=Se.get(w).programs;L!==void 0&&(L.forEach(function(B){Me.releaseProgram(B)}),w.isShaderMaterial&&Me.releaseShaderCache(w))}this.renderBufferDirect=function(w,L,B,H,F,ie){L===null&&(L=nt);let ue=F.isMesh&&F.matrixWorld.determinant()<0,ye=AE(w,L,B,H,F);Ce.setMaterial(H,ue);let ve=B.index,Ie=1;if(H.wireframe===!0){if(ve=K.getWireframeAttribute(B),ve===void 0)return;Ie=2}let Le=B.drawRange,_e=B.attributes.position,et=Le.start*Ie,yt=(Le.start+Le.count)*Ie;ie!==null&&(et=Math.max(et,ie.start*Ie),yt=Math.min(yt,(ie.start+ie.count)*Ie)),ve!==null?(et=Math.max(et,0),yt=Math.min(yt,ve.count)):_e!=null&&(et=Math.max(et,0),yt=Math.min(yt,_e.count));let Mt=yt-et;if(Mt<0||Mt===1/0)return;gt.setup(F,H,ye,B,ve);let on,it=me;if(ve!==null&&(on=Y.get(ve),it=Ke,it.setIndex(on)),F.isMesh)H.wireframe===!0?(Ce.setLineWidth(H.wireframeLinewidth*_t()),it.setMode(k.LINES)):it.setMode(k.TRIANGLES);else if(F.isLine){let Ee=H.linewidth;Ee===void 0&&(Ee=1),Ce.setLineWidth(Ee*_t()),F.isLineSegments?it.setMode(k.LINES):F.isLineLoop?it.setMode(k.LINE_LOOP):it.setMode(k.LINE_STRIP)}else F.isPoints?it.setMode(k.POINTS):F.isSprite&&it.setMode(k.TRIANGLES);if(F.isBatchedMesh)if(F._multiDrawInstances!==null)it.renderMultiDrawInstances(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount,F._multiDrawInstances);else if(Ye.get("WEBGL_multi_draw"))it.renderMultiDraw(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount);else{let Ee=F._multiDrawStarts,vi=F._multiDrawCounts,rt=F._multiDrawCount,Ln=ve?Y.get(ve).bytesPerElement:1,os=Se.get(H).currentProgram.getUniforms();for(let hn=0;hn<rt;hn++)os.setValue(k,"_gl_DrawID",hn),it.render(Ee[hn]/Ln,vi[hn])}else if(F.isInstancedMesh)it.renderInstances(et,Mt,F.count);else if(B.isInstancedBufferGeometry){let Ee=B._maxInstanceCount!==void 0?B._maxInstanceCount:1/0,vi=Math.min(B.instanceCount,Ee);it.renderInstances(et,Mt,vi)}else it.render(et,Mt)};function at(w,L,B){w.transparent===!0&&w.side===un&&w.forceSinglePass===!1?(w.side=Yt,w.needsUpdate=!0,tl(w,L,B),w.side=cr,w.needsUpdate=!0,tl(w,L,B),w.side=un):tl(w,L,B)}this.compile=function(w,L,B=null){B===null&&(B=w),p=Qe.get(B),p.init(L),_.push(p),B.traverseVisible(function(F){F.isLight&&F.layers.test(L.layers)&&(p.pushLight(F),F.castShadow&&p.pushShadow(F))}),w!==B&&w.traverseVisible(function(F){F.isLight&&F.layers.test(L.layers)&&(p.pushLight(F),F.castShadow&&p.pushShadow(F))}),p.setupLights();let H=new Set;return w.traverse(function(F){if(!(F.isMesh||F.isPoints||F.isLine||F.isSprite))return;let ie=F.material;if(ie)if(Array.isArray(ie))for(let ue=0;ue<ie.length;ue++){let ye=ie[ue];at(ye,B,F),H.add(ye)}else at(ie,B,F),H.add(ie)}),_.pop(),p=null,H},this.compileAsync=function(w,L,B=null){let H=this.compile(w,L,B);return new Promise(F=>{function ie(){if(H.forEach(function(ue){Se.get(ue).currentProgram.isReady()&&H.delete(ue)}),H.size===0){F(w);return}setTimeout(ie,10)}Ye.get("KHR_parallel_shader_compile")!==null?ie():setTimeout(ie,10)})};let On=null;function yi(w){On&&On(w)}function Yg(){dr.stop()}function Zg(){dr.start()}let dr=new tE;dr.setAnimationLoop(yi),typeof self<"u"&&dr.setContext(self),this.setAnimationLoop=function(w){On=w,j.setAnimationLoop(w),w===null?dr.stop():dr.start()},j.addEventListener("sessionstart",Yg),j.addEventListener("sessionend",Zg),this.render=function(w,L){if(L!==void 0&&L.isCamera!==!0){console.error("THREE.WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(A===!0)return;if(w.matrixWorldAutoUpdate===!0&&w.updateMatrixWorld(),L.parent===null&&L.matrixWorldAutoUpdate===!0&&L.updateMatrixWorld(),j.enabled===!0&&j.isPresenting===!0&&(j.cameraAutoUpdate===!0&&j.updateCamera(L),L=j.getCamera()),w.isScene===!0&&w.onBeforeRender(y,w,L,I),p=Qe.get(w,_.length),p.init(L),_.push(p),be.multiplyMatrices(L.projectionMatrix,L.matrixWorldInverse),G.setFromProjectionMatrix(be),fe=this.localClippingEnabled,J=ne.init(this.clippingPlanes,fe),g=he.get(w,M.length),g.init(),M.push(g),j.enabled===!0&&j.isPresenting===!0){let ie=y.xr.getDepthSensingMesh();ie!==null&&xd(ie,L,-1/0,y.sortObjects)}xd(w,L,0,y.sortObjects),g.finish(),y.sortObjects===!0&&g.sort(Q,ee),ze=j.enabled===!1||j.isPresenting===!1||j.hasDepthSensing()===!1,ze&&De.addToRenderList(g,w),this.info.render.frame++,J===!0&&ne.beginShadows();let B=p.state.shadowsArray;pe.render(B,w,L),J===!0&&ne.endShadows(),this.info.autoReset===!0&&this.info.reset();let H=g.opaque,F=g.transmissive;if(p.setupLights(),L.isArrayCamera){let ie=L.cameras;if(F.length>0)for(let ue=0,ye=ie.length;ue<ye;ue++){let ve=ie[ue];Jg(H,F,w,ve)}ze&&De.render(w);for(let ue=0,ye=ie.length;ue<ye;ue++){let ve=ie[ue];Kg(g,w,ve,ve.viewport)}}else F.length>0&&Jg(H,F,w,L),ze&&De.render(w),Kg(g,w,L);I!==null&&(T.updateMultisampleRenderTarget(I),T.updateRenderTargetMipmap(I)),w.isScene===!0&&w.onAfterRender(y,w,L),gt.resetDefaultState(),b=-1,x=null,_.pop(),_.length>0?(p=_[_.length-1],J===!0&&ne.setGlobalState(y.clippingPlanes,p.state.camera)):p=null,M.pop(),M.length>0?g=M[M.length-1]:g=null};function xd(w,L,B,H){if(w.visible===!1)return;if(w.layers.test(L.layers)){if(w.isGroup)B=w.renderOrder;else if(w.isLOD)w.autoUpdate===!0&&w.update(L);else if(w.isLight)p.pushLight(w),w.castShadow&&p.pushShadow(w);else if(w.isSprite){if(!w.frustumCulled||G.intersectsSprite(w)){H&&Ne.setFromMatrixPosition(w.matrixWorld).applyMatrix4(be);let ue=X.update(w),ye=w.material;ye.visible&&g.push(w,ue,ye,B,Ne.z,null)}}else if((w.isMesh||w.isLine||w.isPoints)&&(!w.frustumCulled||G.intersectsObject(w))){let ue=X.update(w),ye=w.material;if(H&&(w.boundingSphere!==void 0?(w.boundingSphere===null&&w.computeBoundingSphere(),Ne.copy(w.boundingSphere.center)):(ue.boundingSphere===null&&ue.computeBoundingSphere(),Ne.copy(ue.boundingSphere.center)),Ne.applyMatrix4(w.matrixWorld).applyMatrix4(be)),Array.isArray(ye)){let ve=ue.groups;for(let Ie=0,Le=ve.length;Ie<Le;Ie++){let _e=ve[Ie],et=ye[_e.materialIndex];et&&et.visible&&g.push(w,ue,et,B,Ne.z,_e)}}else ye.visible&&g.push(w,ue,ye,B,Ne.z,null)}}let ie=w.children;for(let ue=0,ye=ie.length;ue<ye;ue++)xd(ie[ue],L,B,H)}function Kg(w,L,B,H){let F=w.opaque,ie=w.transmissive,ue=w.transparent;p.setupLightsView(B),J===!0&&ne.setGlobalState(y.clippingPlanes,B),H&&Ce.viewport(D.copy(H)),F.length>0&&el(F,L,B),ie.length>0&&el(ie,L,B),ue.length>0&&el(ue,L,B),Ce.buffers.depth.setTest(!0),Ce.buffers.depth.setMask(!0),Ce.buffers.color.setMask(!0),Ce.setPolygonOffset(!1)}function Jg(w,L,B,H){if((B.isScene===!0?B.overrideMaterial:null)!==null)return;p.state.transmissionRenderTarget[H.id]===void 0&&(p.state.transmissionRenderTarget[H.id]=new xn(1,1,{generateMipmaps:!0,type:Ye.has("EXT_color_buffer_half_float")||Ye.has("EXT_color_buffer_float")?ns:Ui,minFilter:Gr,samples:4,stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:Xe.workingColorSpace}));let ie=p.state.transmissionRenderTarget[H.id],ue=H.viewport||D;ie.setSize(ue.z,ue.w);let ye=y.getRenderTarget();y.setRenderTarget(ie),y.getClearColor(U),$=y.getClearAlpha(),$<1&&y.setClearColor(16777215,.5),y.clear(),ze&&De.render(B);let ve=y.toneMapping;y.toneMapping=pi;let Ie=H.viewport;if(H.viewport!==void 0&&(H.viewport=void 0),p.setupLightsView(H),J===!0&&ne.setGlobalState(y.clippingPlanes,H),el(w,B,H),T.updateMultisampleRenderTarget(ie),T.updateRenderTargetMipmap(ie),Ye.has("WEBGL_multisampled_render_to_texture")===!1){let Le=!1;for(let _e=0,et=L.length;_e<et;_e++){let yt=L[_e],Mt=yt.object,on=yt.geometry,it=yt.material,Ee=yt.group;if(it.side===un&&Mt.layers.test(H.layers)){let vi=it.side;it.side=Yt,it.needsUpdate=!0,Qg(Mt,B,H,on,it,Ee),it.side=vi,it.needsUpdate=!0,Le=!0}}Le===!0&&(T.updateMultisampleRenderTarget(ie),T.updateRenderTargetMipmap(ie))}y.setRenderTarget(ye),y.setClearColor(U,$),Ie!==void 0&&(H.viewport=Ie),y.toneMapping=ve}function el(w,L,B){let H=L.isScene===!0?L.overrideMaterial:null;for(let F=0,ie=w.length;F<ie;F++){let ue=w[F],ye=ue.object,ve=ue.geometry,Ie=H===null?ue.material:H,Le=ue.group;ye.layers.test(B.layers)&&Qg(ye,L,B,ve,Ie,Le)}}function Qg(w,L,B,H,F,ie){w.onBeforeRender(y,L,B,H,F,ie),w.modelViewMatrix.multiplyMatrices(B.matrixWorldInverse,w.matrixWorld),w.normalMatrix.getNormalMatrix(w.modelViewMatrix),F.onBeforeRender(y,L,B,H,w,ie),F.transparent===!0&&F.side===un&&F.forceSinglePass===!1?(F.side=Yt,F.needsUpdate=!0,y.renderBufferDirect(B,L,H,F,w,ie),F.side=cr,F.needsUpdate=!0,y.renderBufferDirect(B,L,H,F,w,ie),F.side=un):y.renderBufferDirect(B,L,H,F,w,ie),w.onAfterRender(y,L,B,H,F,ie)}function tl(w,L,B){L.isScene!==!0&&(L=nt);let H=Se.get(w),F=p.state.lights,ie=p.state.shadowsArray,ue=F.state.version,ye=Me.getParameters(w,F.state,ie,L,B),ve=Me.getProgramCacheKey(ye),Ie=H.programs;H.environment=w.isMeshStandardMaterial?L.environment:null,H.fog=L.fog,H.envMap=(w.isMeshStandardMaterial?V:E).get(w.envMap||H.environment),H.envMapRotation=H.environment!==null&&w.envMap===null?L.environmentRotation:w.envMapRotation,Ie===void 0&&(w.addEventListener("dispose",Oe),Ie=new Map,H.programs=Ie);let Le=Ie.get(ve);if(Le!==void 0){if(H.currentProgram===Le&&H.lightsStateVersion===ue)return ty(w,ye),Le}else ye.uniforms=Me.getUniforms(w),w.onBeforeCompile(ye,y),Le=Me.acquireProgram(ye,ve),Ie.set(ve,Le),H.uniforms=ye.uniforms;let _e=H.uniforms;return(!w.isShaderMaterial&&!w.isRawShaderMaterial||w.clipping===!0)&&(_e.clippingPlanes=ne.uniform),ty(w,ye),H.needsLights=RE(w),H.lightsStateVersion=ue,H.needsLights&&(_e.ambientLightColor.value=F.state.ambient,_e.lightProbe.value=F.state.probe,_e.directionalLights.value=F.state.directional,_e.directionalLightShadows.value=F.state.directionalShadow,_e.spotLights.value=F.state.spot,_e.spotLightShadows.value=F.state.spotShadow,_e.rectAreaLights.value=F.state.rectArea,_e.ltc_1.value=F.state.rectAreaLTC1,_e.ltc_2.value=F.state.rectAreaLTC2,_e.pointLights.value=F.state.point,_e.pointLightShadows.value=F.state.pointShadow,_e.hemisphereLights.value=F.state.hemi,_e.directionalShadowMap.value=F.state.directionalShadowMap,_e.directionalShadowMatrix.value=F.state.directionalShadowMatrix,_e.spotShadowMap.value=F.state.spotShadowMap,_e.spotLightMatrix.value=F.state.spotLightMatrix,_e.spotLightMap.value=F.state.spotLightMap,_e.pointShadowMap.value=F.state.pointShadowMap,_e.pointShadowMatrix.value=F.state.pointShadowMatrix),H.currentProgram=Le,H.uniformsList=null,Le}function ey(w){if(w.uniformsList===null){let L=w.currentProgram.getUniforms();w.uniformsList=co.seqWithValue(L.seq,w.uniforms)}return w.uniformsList}function ty(w,L){let B=Se.get(w);B.outputColorSpace=L.outputColorSpace,B.batching=L.batching,B.batchingColor=L.batchingColor,B.instancing=L.instancing,B.instancingColor=L.instancingColor,B.instancingMorph=L.instancingMorph,B.skinning=L.skinning,B.morphTargets=L.morphTargets,B.morphNormals=L.morphNormals,B.morphColors=L.morphColors,B.morphTargetsCount=L.morphTargetsCount,B.numClippingPlanes=L.numClippingPlanes,B.numIntersection=L.numClipIntersection,B.vertexAlphas=L.vertexAlphas,B.vertexTangents=L.vertexTangents,B.toneMapping=L.toneMapping}function AE(w,L,B,H,F){L.isScene!==!0&&(L=nt),T.resetTextureUnits();let ie=L.fog,ue=H.isMeshStandardMaterial?L.environment:null,ye=I===null?y.outputColorSpace:I.isXRRenderTarget===!0?I.texture.colorSpace:So,ve=(H.isMeshStandardMaterial?V:E).get(H.envMap||ue),Ie=H.vertexColors===!0&&!!B.attributes.color&&B.attributes.color.itemSize===4,Le=!!B.attributes.tangent&&(!!H.normalMap||H.anisotropy>0),_e=!!B.morphAttributes.position,et=!!B.morphAttributes.normal,yt=!!B.morphAttributes.color,Mt=pi;H.toneMapped&&(I===null||I.isXRRenderTarget===!0)&&(Mt=y.toneMapping);let on=B.morphAttributes.position||B.morphAttributes.normal||B.morphAttributes.color,it=on!==void 0?on.length:0,Ee=Se.get(H),vi=p.state.lights;if(J===!0&&(fe===!0||w!==x)){let Sn=w===x&&H.id===b;ne.setState(H,w,Sn)}let rt=!1;H.version===Ee.__version?(Ee.needsLights&&Ee.lightsStateVersion!==vi.state.version||Ee.outputColorSpace!==ye||F.isBatchedMesh&&Ee.batching===!1||!F.isBatchedMesh&&Ee.batching===!0||F.isBatchedMesh&&Ee.batchingColor===!0&&F.colorTexture===null||F.isBatchedMesh&&Ee.batchingColor===!1&&F.colorTexture!==null||F.isInstancedMesh&&Ee.instancing===!1||!F.isInstancedMesh&&Ee.instancing===!0||F.isSkinnedMesh&&Ee.skinning===!1||!F.isSkinnedMesh&&Ee.skinning===!0||F.isInstancedMesh&&Ee.instancingColor===!0&&F.instanceColor===null||F.isInstancedMesh&&Ee.instancingColor===!1&&F.instanceColor!==null||F.isInstancedMesh&&Ee.instancingMorph===!0&&F.morphTexture===null||F.isInstancedMesh&&Ee.instancingMorph===!1&&F.morphTexture!==null||Ee.envMap!==ve||H.fog===!0&&Ee.fog!==ie||Ee.numClippingPlanes!==void 0&&(Ee.numClippingPlanes!==ne.numPlanes||Ee.numIntersection!==ne.numIntersection)||Ee.vertexAlphas!==Ie||Ee.vertexTangents!==Le||Ee.morphTargets!==_e||Ee.morphNormals!==et||Ee.morphColors!==yt||Ee.toneMapping!==Mt||Ee.morphTargetsCount!==it)&&(rt=!0):(rt=!0,Ee.__version=H.version);let Ln=Ee.currentProgram;rt===!0&&(Ln=tl(H,L,F));let os=!1,hn=!1,Ro=!1,Et=Ln.getUniforms(),Zn=Ee.uniforms;if(Ce.useProgram(Ln.program)&&(os=!0,hn=!0,Ro=!0),H.id!==b&&(b=H.id,hn=!0),os||x!==w){Ce.buffers.depth.getReversed()?(re.copy(w.projectionMatrix),WI(re),jI(re),Et.setValue(k,"projectionMatrix",re)):Et.setValue(k,"projectionMatrix",w.projectionMatrix),Et.setValue(k,"viewMatrix",w.matrixWorldInverse);let Gi=Et.map.cameraPosition;Gi!==void 0&&Gi.setValue(k,Te.setFromMatrixPosition(w.matrixWorld)),Ze.logarithmicDepthBuffer&&Et.setValue(k,"logDepthBufFC",2/(Math.log(w.far+1)/Math.LN2)),(H.isMeshPhongMaterial||H.isMeshToonMaterial||H.isMeshLambertMaterial||H.isMeshBasicMaterial||H.isMeshStandardMaterial||H.isShaderMaterial)&&Et.setValue(k,"isOrthographic",w.isOrthographicCamera===!0),x!==w&&(x=w,hn=!0,Ro=!0)}if(F.isSkinnedMesh){Et.setOptional(k,F,"bindMatrix"),Et.setOptional(k,F,"bindMatrixInverse");let Sn=F.skeleton;Sn&&(Sn.boneTexture===null&&Sn.computeBoneTexture(),Et.setValue(k,"boneTexture",Sn.boneTexture,T))}F.isBatchedMesh&&(Et.setOptional(k,F,"batchingTexture"),Et.setValue(k,"batchingTexture",F._matricesTexture,T),Et.setOptional(k,F,"batchingIdTexture"),Et.setValue(k,"batchingIdTexture",F._indirectTexture,T),Et.setOptional(k,F,"batchingColorTexture"),F._colorsTexture!==null&&Et.setValue(k,"batchingColorTexture",F._colorsTexture,T));let No=B.morphAttributes;if((No.position!==void 0||No.normal!==void 0||No.color!==void 0)&&Ae.update(F,B,Ln),(hn||Ee.receiveShadow!==F.receiveShadow)&&(Ee.receiveShadow=F.receiveShadow,Et.setValue(k,"receiveShadow",F.receiveShadow)),H.isMeshGouraudMaterial&&H.envMap!==null&&(Zn.envMap.value=ve,Zn.flipEnvMap.value=ve.isCubeTexture&&ve.isRenderTargetTexture===!1?-1:1),H.isMeshStandardMaterial&&H.envMap===null&&L.environment!==null&&(Zn.envMapIntensity.value=L.environmentIntensity),hn&&(Et.setValue(k,"toneMappingExposure",y.toneMappingExposure),Ee.needsLights&&IE(Zn,Ro),ie&&H.fog===!0&&le.refreshFogUniforms(Zn,ie),le.refreshMaterialUniforms(Zn,H,z,q,p.state.transmissionRenderTarget[w.id]),co.upload(k,ey(Ee),Zn,T)),H.isShaderMaterial&&H.uniformsNeedUpdate===!0&&(co.upload(k,ey(Ee),Zn,T),H.uniformsNeedUpdate=!1),H.isSpriteMaterial&&Et.setValue(k,"center",F.center),Et.setValue(k,"modelViewMatrix",F.modelViewMatrix),Et.setValue(k,"normalMatrix",F.normalMatrix),Et.setValue(k,"modelMatrix",F.matrixWorld),H.isShaderMaterial||H.isRawShaderMaterial){let Sn=H.uniformsGroups;for(let Gi=0,Wi=Sn.length;Gi<Wi;Gi++){let ny=Sn[Gi];P.update(ny,Ln),P.bind(ny,Ln)}}return Ln}function IE(w,L){w.ambientLightColor.needsUpdate=L,w.lightProbe.needsUpdate=L,w.directionalLights.needsUpdate=L,w.directionalLightShadows.needsUpdate=L,w.pointLights.needsUpdate=L,w.pointLightShadows.needsUpdate=L,w.spotLights.needsUpdate=L,w.spotLightShadows.needsUpdate=L,w.rectAreaLights.needsUpdate=L,w.hemisphereLights.needsUpdate=L}function RE(w){return w.isMeshLambertMaterial||w.isMeshToonMaterial||w.isMeshPhongMaterial||w.isMeshStandardMaterial||w.isShadowMaterial||w.isShaderMaterial&&w.lights===!0}this.getActiveCubeFace=function(){return C},this.getActiveMipmapLevel=function(){return S},this.getRenderTarget=function(){return I},this.setRenderTargetTextures=function(w,L,B){Se.get(w.texture).__webglTexture=L,Se.get(w.depthTexture).__webglTexture=B;let H=Se.get(w);H.__hasExternalTextures=!0,H.__autoAllocateDepthBuffer=B===void 0,H.__autoAllocateDepthBuffer||Ye.has("WEBGL_multisampled_render_to_texture")===!0&&(console.warn("THREE.WebGLRenderer: Render-to-texture extension was disabled because an external texture was provided"),H.__useRenderToTexture=!1)},this.setRenderTargetFramebuffer=function(w,L){let B=Se.get(w);B.__webglFramebuffer=L,B.__useDefaultFramebuffer=L===void 0},this.setRenderTarget=function(w,L=0,B=0){I=w,C=L,S=B;let H=!0,F=null,ie=!1,ue=!1;if(w){let ve=Se.get(w);if(ve.__useDefaultFramebuffer!==void 0)Ce.bindFramebuffer(k.FRAMEBUFFER,null),H=!1;else if(ve.__webglFramebuffer===void 0)T.setupRenderTarget(w);else if(ve.__hasExternalTextures)T.rebindTextures(w,Se.get(w.texture).__webglTexture,Se.get(w.depthTexture).__webglTexture);else if(w.depthBuffer){let _e=w.depthTexture;if(ve.__boundDepthTexture!==_e){if(_e!==null&&Se.has(_e)&&(w.width!==_e.image.width||w.height!==_e.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");T.setupDepthRenderbuffer(w)}}let Ie=w.texture;(Ie.isData3DTexture||Ie.isDataArrayTexture||Ie.isCompressedArrayTexture)&&(ue=!0);let Le=Se.get(w).__webglFramebuffer;w.isWebGLCubeRenderTarget?(Array.isArray(Le[L])?F=Le[L][B]:F=Le[L],ie=!0):w.samples>0&&T.useMultisampledRTT(w)===!1?F=Se.get(w).__webglMultisampledFramebuffer:Array.isArray(Le)?F=Le[B]:F=Le,D.copy(w.viewport),N.copy(w.scissor),O=w.scissorTest}else D.copy(oe).multiplyScalar(z).floor(),N.copy(ge).multiplyScalar(z).floor(),O=we;if(Ce.bindFramebuffer(k.FRAMEBUFFER,F)&&H&&Ce.drawBuffers(w,F),Ce.viewport(D),Ce.scissor(N),Ce.setScissorTest(O),ie){let ve=Se.get(w.texture);k.framebufferTexture2D(k.FRAMEBUFFER,k.COLOR_ATTACHMENT0,k.TEXTURE_CUBE_MAP_POSITIVE_X+L,ve.__webglTexture,B)}else if(ue){let ve=Se.get(w.texture),Ie=L||0;k.framebufferTextureLayer(k.FRAMEBUFFER,k.COLOR_ATTACHMENT0,ve.__webglTexture,B||0,Ie)}b=-1},this.readRenderTargetPixels=function(w,L,B,H,F,ie,ue){if(!(w&&w.isWebGLRenderTarget)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let ye=Se.get(w).__webglFramebuffer;if(w.isWebGLCubeRenderTarget&&ue!==void 0&&(ye=ye[ue]),ye){Ce.bindFramebuffer(k.FRAMEBUFFER,ye);try{let ve=w.texture,Ie=ve.format,Le=ve.type;if(!Ze.textureFormatReadable(Ie)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!Ze.textureTypeReadable(Le)){console.error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}L>=0&&L<=w.width-H&&B>=0&&B<=w.height-F&&k.readPixels(L,B,H,F,ke.convert(Ie),ke.convert(Le),ie)}finally{let ve=I!==null?Se.get(I).__webglFramebuffer:null;Ce.bindFramebuffer(k.FRAMEBUFFER,ve)}}},this.readRenderTargetPixelsAsync=function(w,L,B,H,F,ie,ue){return jt(this,null,function*(){if(!(w&&w.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let ye=Se.get(w).__webglFramebuffer;if(w.isWebGLCubeRenderTarget&&ue!==void 0&&(ye=ye[ue]),ye){let ve=w.texture,Ie=ve.format,Le=ve.type;if(!Ze.textureFormatReadable(Ie))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!Ze.textureTypeReadable(Le))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");if(L>=0&&L<=w.width-H&&B>=0&&B<=w.height-F){Ce.bindFramebuffer(k.FRAMEBUFFER,ye);let _e=k.createBuffer();k.bindBuffer(k.PIXEL_PACK_BUFFER,_e),k.bufferData(k.PIXEL_PACK_BUFFER,ie.byteLength,k.STREAM_READ),k.readPixels(L,B,H,F,ke.convert(Ie),ke.convert(Le),0);let et=I!==null?Se.get(I).__webglFramebuffer:null;Ce.bindFramebuffer(k.FRAMEBUFFER,et);let yt=k.fenceSync(k.SYNC_GPU_COMMANDS_COMPLETE,0);return k.flush(),yield GI(k,yt,4),k.bindBuffer(k.PIXEL_PACK_BUFFER,_e),k.getBufferSubData(k.PIXEL_PACK_BUFFER,0,ie),k.deleteBuffer(_e),k.deleteSync(yt),ie}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")}})},this.copyFramebufferToTexture=function(w,L=null,B=0){w.isTexture!==!0&&(Aa("WebGLRenderer: copyFramebufferToTexture function signature has changed."),L=arguments[0]||null,w=arguments[1]);let H=Math.pow(2,-B),F=Math.floor(w.image.width*H),ie=Math.floor(w.image.height*H),ue=L!==null?L.x:0,ye=L!==null?L.y:0;T.setTexture2D(w,0),k.copyTexSubImage2D(k.TEXTURE_2D,B,0,0,ue,ye,F,ie),Ce.unbindTexture()},this.copyTextureToTexture=function(w,L,B=null,H=null,F=0){w.isTexture!==!0&&(Aa("WebGLRenderer: copyTextureToTexture function signature has changed."),H=arguments[0]||null,w=arguments[1],L=arguments[2],F=arguments[3]||0,B=null);let ie,ue,ye,ve,Ie,Le,_e,et,yt,Mt=w.isCompressedTexture?w.mipmaps[F]:w.image;B!==null?(ie=B.max.x-B.min.x,ue=B.max.y-B.min.y,ye=B.isBox3?B.max.z-B.min.z:1,ve=B.min.x,Ie=B.min.y,Le=B.isBox3?B.min.z:0):(ie=Mt.width,ue=Mt.height,ye=Mt.depth||1,ve=0,Ie=0,Le=0),H!==null?(_e=H.x,et=H.y,yt=H.z):(_e=0,et=0,yt=0);let on=ke.convert(L.format),it=ke.convert(L.type),Ee;L.isData3DTexture?(T.setTexture3D(L,0),Ee=k.TEXTURE_3D):L.isDataArrayTexture||L.isCompressedArrayTexture?(T.setTexture2DArray(L,0),Ee=k.TEXTURE_2D_ARRAY):(T.setTexture2D(L,0),Ee=k.TEXTURE_2D),k.pixelStorei(k.UNPACK_FLIP_Y_WEBGL,L.flipY),k.pixelStorei(k.UNPACK_PREMULTIPLY_ALPHA_WEBGL,L.premultiplyAlpha),k.pixelStorei(k.UNPACK_ALIGNMENT,L.unpackAlignment);let vi=k.getParameter(k.UNPACK_ROW_LENGTH),rt=k.getParameter(k.UNPACK_IMAGE_HEIGHT),Ln=k.getParameter(k.UNPACK_SKIP_PIXELS),os=k.getParameter(k.UNPACK_SKIP_ROWS),hn=k.getParameter(k.UNPACK_SKIP_IMAGES);k.pixelStorei(k.UNPACK_ROW_LENGTH,Mt.width),k.pixelStorei(k.UNPACK_IMAGE_HEIGHT,Mt.height),k.pixelStorei(k.UNPACK_SKIP_PIXELS,ve),k.pixelStorei(k.UNPACK_SKIP_ROWS,Ie),k.pixelStorei(k.UNPACK_SKIP_IMAGES,Le);let Ro=w.isDataArrayTexture||w.isData3DTexture,Et=L.isDataArrayTexture||L.isData3DTexture;if(w.isRenderTargetTexture||w.isDepthTexture){let Zn=Se.get(w),No=Se.get(L),Sn=Se.get(Zn.__renderTarget),Gi=Se.get(No.__renderTarget);Ce.bindFramebuffer(k.READ_FRAMEBUFFER,Sn.__webglFramebuffer),Ce.bindFramebuffer(k.DRAW_FRAMEBUFFER,Gi.__webglFramebuffer);for(let Wi=0;Wi<ye;Wi++)Ro&&k.framebufferTextureLayer(k.READ_FRAMEBUFFER,k.COLOR_ATTACHMENT0,Se.get(w).__webglTexture,F,Le+Wi),w.isDepthTexture?(Et&&k.framebufferTextureLayer(k.DRAW_FRAMEBUFFER,k.COLOR_ATTACHMENT0,Se.get(L).__webglTexture,F,yt+Wi),k.blitFramebuffer(ve,Ie,ie,ue,_e,et,ie,ue,k.DEPTH_BUFFER_BIT,k.NEAREST)):Et?k.copyTexSubImage3D(Ee,F,_e,et,yt+Wi,ve,Ie,ie,ue):k.copyTexSubImage2D(Ee,F,_e,et,yt+Wi,ve,Ie,ie,ue);Ce.bindFramebuffer(k.READ_FRAMEBUFFER,null),Ce.bindFramebuffer(k.DRAW_FRAMEBUFFER,null)}else Et?w.isDataTexture||w.isData3DTexture?k.texSubImage3D(Ee,F,_e,et,yt,ie,ue,ye,on,it,Mt.data):L.isCompressedArrayTexture?k.compressedTexSubImage3D(Ee,F,_e,et,yt,ie,ue,ye,on,Mt.data):k.texSubImage3D(Ee,F,_e,et,yt,ie,ue,ye,on,it,Mt):w.isDataTexture?k.texSubImage2D(k.TEXTURE_2D,F,_e,et,ie,ue,on,it,Mt.data):w.isCompressedTexture?k.compressedTexSubImage2D(k.TEXTURE_2D,F,_e,et,Mt.width,Mt.height,on,Mt.data):k.texSubImage2D(k.TEXTURE_2D,F,_e,et,ie,ue,on,it,Mt);k.pixelStorei(k.UNPACK_ROW_LENGTH,vi),k.pixelStorei(k.UNPACK_IMAGE_HEIGHT,rt),k.pixelStorei(k.UNPACK_SKIP_PIXELS,Ln),k.pixelStorei(k.UNPACK_SKIP_ROWS,os),k.pixelStorei(k.UNPACK_SKIP_IMAGES,hn),F===0&&L.generateMipmaps&&k.generateMipmap(Ee),Ce.unbindTexture()},this.copyTextureToTexture3D=function(w,L,B=null,H=null,F=0){return w.isTexture!==!0&&(Aa("WebGLRenderer: copyTextureToTexture3D function signature has changed."),B=arguments[0]||null,H=arguments[1]||null,w=arguments[2],L=arguments[3],F=arguments[4]||0),Aa('WebGLRenderer: copyTextureToTexture3D function has been deprecated. Use "copyTextureToTexture" instead.'),this.copyTextureToTexture(w,L,B,H,F)},this.initRenderTarget=function(w){Se.get(w).__webglFramebuffer===void 0&&T.setupRenderTarget(w)},this.initTexture=function(w){w.isCubeTexture?T.setTextureCube(w,0):w.isData3DTexture?T.setTexture3D(w,0):w.isDataArrayTexture||w.isCompressedArrayTexture?T.setTexture2DArray(w,0):T.setTexture2D(w,0),Ce.unbindTexture()},this.resetState=function(){C=0,S=0,I=null,Ce.reset(),gt.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return Fi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorspace=Xe._getDrawingBufferColorSpace(e),t.unpackColorSpace=Xe._getUnpackColorSpace()}},Lu=class n{constructor(e,t=25e-5){this.isFogExp2=!0,this.name="",this.color=new xe(e),this.density=t}clone(){return new n(this.color,this.density)}toJSON(){return{type:"FogExp2",name:this.name,color:this.color.getHex(),density:this.density}}};var Fu=class extends qn{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new $r,this.environmentIntensity=1,this.environmentRotation=new $r,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},eg=class{constructor(e,t){this.isInterleavedBuffer=!0,this.array=e,this.stride=t,this.count=e!==void 0?e.length/t:0,this.usage=km,this.updateRanges=[],this.version=0,this.uuid=mi()}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.array=new e.array.constructor(e.array),this.count=e.count,this.stride=e.stride,this.usage=e.usage,this}copyAt(e,t,i){e*=this.stride,i*=t.stride;for(let r=0,s=this.stride;r<s;r++)this.array[e+r]=t.array[i+r];return this}set(e,t=0){return this.array.set(e,t),this}clone(e){e.arrayBuffers===void 0&&(e.arrayBuffers={}),this.array.buffer._uuid===void 0&&(this.array.buffer._uuid=mi()),e.arrayBuffers[this.array.buffer._uuid]===void 0&&(e.arrayBuffers[this.array.buffer._uuid]=this.array.slice(0).buffer);let t=new this.array.constructor(e.arrayBuffers[this.array.buffer._uuid]),i=new this.constructor(t,this.stride);return i.setUsage(this.usage),i}onUpload(e){return this.onUploadCallback=e,this}toJSON(e){return e.arrayBuffers===void 0&&(e.arrayBuffers={}),this.array.buffer._uuid===void 0&&(this.array.buffer._uuid=mi()),e.arrayBuffers[this.array.buffer._uuid]===void 0&&(e.arrayBuffers[this.array.buffer._uuid]=Array.from(new Uint32Array(this.array.buffer))),{uuid:this.uuid,buffer:this.array.buffer._uuid,type:this.array.constructor.name,stride:this.stride}}},tn=new R,ku=class n{constructor(e,t,i,r=!1){this.isInterleavedBufferAttribute=!0,this.name="",this.data=e,this.itemSize=t,this.offset=i,this.normalized=r}get count(){return this.data.count}get array(){return this.data.array}set needsUpdate(e){this.data.needsUpdate=e}applyMatrix4(e){for(let t=0,i=this.data.count;t<i;t++)tn.fromBufferAttribute(this,t),tn.applyMatrix4(e),this.setXYZ(t,tn.x,tn.y,tn.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)tn.fromBufferAttribute(this,t),tn.applyNormalMatrix(e),this.setXYZ(t,tn.x,tn.y,tn.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)tn.fromBufferAttribute(this,t),tn.transformDirection(e),this.setXYZ(t,tn.x,tn.y,tn.z);return this}getComponent(e,t){let i=this.array[e*this.data.stride+this.offset+t];return this.normalized&&(i=jn(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=ut(i,this.array)),this.data.array[e*this.data.stride+this.offset+t]=i,this}setX(e,t){return this.normalized&&(t=ut(t,this.array)),this.data.array[e*this.data.stride+this.offset]=t,this}setY(e,t){return this.normalized&&(t=ut(t,this.array)),this.data.array[e*this.data.stride+this.offset+1]=t,this}setZ(e,t){return this.normalized&&(t=ut(t,this.array)),this.data.array[e*this.data.stride+this.offset+2]=t,this}setW(e,t){return this.normalized&&(t=ut(t,this.array)),this.data.array[e*this.data.stride+this.offset+3]=t,this}getX(e){let t=this.data.array[e*this.data.stride+this.offset];return this.normalized&&(t=jn(t,this.array)),t}getY(e){let t=this.data.array[e*this.data.stride+this.offset+1];return this.normalized&&(t=jn(t,this.array)),t}getZ(e){let t=this.data.array[e*this.data.stride+this.offset+2];return this.normalized&&(t=jn(t,this.array)),t}getW(e){let t=this.data.array[e*this.data.stride+this.offset+3];return this.normalized&&(t=jn(t,this.array)),t}setXY(e,t,i){return e=e*this.data.stride+this.offset,this.normalized&&(t=ut(t,this.array),i=ut(i,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this}setXYZ(e,t,i,r){return e=e*this.data.stride+this.offset,this.normalized&&(t=ut(t,this.array),i=ut(i,this.array),r=ut(r,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this.data.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e=e*this.data.stride+this.offset,this.normalized&&(t=ut(t,this.array),i=ut(i,this.array),r=ut(r,this.array),s=ut(s,this.array)),this.data.array[e+0]=t,this.data.array[e+1]=i,this.data.array[e+2]=r,this.data.array[e+3]=s,this}clone(e){if(e===void 0){console.log("THREE.InterleavedBufferAttribute.clone(): Cloning an interleaved buffer attribute will de-interleave buffer data.");let t=[];for(let i=0;i<this.count;i++){let r=i*this.data.stride+this.offset;for(let s=0;s<this.itemSize;s++)t.push(this.data.array[r+s])}return new Zt(new this.array.constructor(t),this.itemSize,this.normalized)}else return e.interleavedBuffers===void 0&&(e.interleavedBuffers={}),e.interleavedBuffers[this.data.uuid]===void 0&&(e.interleavedBuffers[this.data.uuid]=this.data.clone(e)),new n(e.interleavedBuffers[this.data.uuid],this.itemSize,this.offset,this.normalized)}toJSON(e){if(e===void 0){console.log("THREE.InterleavedBufferAttribute.toJSON(): Serializing an interleaved buffer attribute will de-interleave buffer data.");let t=[];for(let i=0;i<this.count;i++){let r=i*this.data.stride+this.offset;for(let s=0;s<this.itemSize;s++)t.push(this.data.array[r+s])}return{itemSize:this.itemSize,type:this.array.constructor.name,array:t,normalized:this.normalized}}else return e.interleavedBuffers===void 0&&(e.interleavedBuffers={}),e.interleavedBuffers[this.data.uuid]===void 0&&(e.interleavedBuffers[this.data.uuid]=this.data.toJSON(e)),{isInterleavedBufferAttribute:!0,itemSize:this.itemSize,data:this.data.uuid,offset:this.offset,normalized:this.normalized}}},_o=class extends Hi{static get type(){return"SpriteMaterial"}constructor(e){super(),this.isSpriteMaterial=!0,this.color=new xe(16777215),this.map=null,this.alphaMap=null,this.rotation=0,this.sizeAttenuation=!0,this.transparent=!0,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.alphaMap=e.alphaMap,this.rotation=e.rotation,this.sizeAttenuation=e.sizeAttenuation,this.fog=e.fog,this}},Qs,wa=new R,eo=new R,to=new R,no=new te,Sa=new te,oE=new tt,uu=new R,Ca=new R,du=new R,TM=new te,$p=new te,DM=new te,Ha=class extends qn{constructor(e=new _o){if(super(),this.isSprite=!0,this.type="Sprite",Qs===void 0){Qs=new zt;let t=new Float32Array([-.5,-.5,0,0,0,.5,-.5,0,1,0,.5,.5,0,1,1,-.5,.5,0,0,1]),i=new eg(t,5);Qs.setIndex([0,1,2,0,2,3]),Qs.setAttribute("position",new ku(i,3,0,!1)),Qs.setAttribute("uv",new ku(i,2,3,!1))}this.geometry=Qs,this.material=e,this.center=new te(.5,.5)}raycast(e,t){e.camera===null&&console.error('THREE.Sprite: "Raycaster.camera" needs to be set in order to raycast against sprites.'),eo.setFromMatrixScale(this.matrixWorld),oE.copy(e.camera.matrixWorld),this.modelViewMatrix.multiplyMatrices(e.camera.matrixWorldInverse,this.matrixWorld),to.setFromMatrixPosition(this.modelViewMatrix),e.camera.isPerspectiveCamera&&this.material.sizeAttenuation===!1&&eo.multiplyScalar(-to.z);let i=this.material.rotation,r,s;i!==0&&(s=Math.cos(i),r=Math.sin(i));let o=this.center;fu(uu.set(-.5,-.5,0),to,o,eo,r,s),fu(Ca.set(.5,-.5,0),to,o,eo,r,s),fu(du.set(.5,.5,0),to,o,eo,r,s),TM.set(0,0),$p.set(1,0),DM.set(1,1);let a=e.ray.intersectTriangle(uu,Ca,du,!1,wa);if(a===null&&(fu(Ca.set(-.5,.5,0),to,o,eo,r,s),$p.set(0,1),a=e.ray.intersectTriangle(uu,du,Ca,!1,wa),a===null))return;let l=e.ray.origin.distanceTo(wa);l<e.near||l>e.far||t.push({distance:l,point:wa.clone(),uv:lr.getInterpolation(wa,uu,Ca,du,TM,$p,DM,new te),face:null,object:this})}copy(e,t){return super.copy(e,t),e.center!==void 0&&this.center.copy(e.center),this.material=e.material,this}};function fu(n,e,t,i,r,s){no.subVectors(n,t).addScalar(.5).multiply(i),r!==void 0?(Sa.x=s*no.x-r*no.y,Sa.y=r*no.x+s*no.y):Sa.copy(no),n.copy(e),n.x+=Sa.x,n.y+=Sa.y,n.applyMatrix4(oE)}var tg=class extends zi{constructor(e=null,t=1,i=1,r,s,o,a,l,c=_n,u=_n,d,h){super(null,o,a,l,c,u,r,s,d,h),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Uu=class extends Zt{constructor(e,t,i,r=1){super(e,t,i),this.isInstancedBufferAttribute=!0,this.meshPerAttribute=r}copy(e){return super.copy(e),this.meshPerAttribute=e.meshPerAttribute,this}toJSON(){let e=super.toJSON();return e.meshPerAttribute=this.meshPerAttribute,e.isInstancedBufferAttribute=!0,e}},io=new tt,AM=new tt,hu=[],IM=new Bi,UO=new tt,Ta=new wt,Da=new jr,Mn=class extends wt{constructor(e,t,i){super(e,t),this.isInstancedMesh=!0,this.instanceMatrix=new Uu(new Float32Array(i*16),16),this.instanceColor=null,this.morphTexture=null,this.count=i,this.boundingBox=null,this.boundingSphere=null;for(let r=0;r<i;r++)this.setMatrixAt(r,UO)}computeBoundingBox(){let e=this.geometry,t=this.count;this.boundingBox===null&&(this.boundingBox=new Bi),e.boundingBox===null&&e.computeBoundingBox(),this.boundingBox.makeEmpty();for(let i=0;i<t;i++)this.getMatrixAt(i,io),IM.copy(e.boundingBox).applyMatrix4(io),this.boundingBox.union(IM)}computeBoundingSphere(){let e=this.geometry,t=this.count;this.boundingSphere===null&&(this.boundingSphere=new jr),e.boundingSphere===null&&e.computeBoundingSphere(),this.boundingSphere.makeEmpty();for(let i=0;i<t;i++)this.getMatrixAt(i,io),Da.copy(e.boundingSphere).applyMatrix4(io),this.boundingSphere.union(Da)}copy(e,t){return super.copy(e,t),this.instanceMatrix.copy(e.instanceMatrix),e.morphTexture!==null&&(this.morphTexture=e.morphTexture.clone()),e.instanceColor!==null&&(this.instanceColor=e.instanceColor.clone()),this.count=e.count,e.boundingBox!==null&&(this.boundingBox=e.boundingBox.clone()),e.boundingSphere!==null&&(this.boundingSphere=e.boundingSphere.clone()),this}getColorAt(e,t){t.fromArray(this.instanceColor.array,e*3)}getMatrixAt(e,t){t.fromArray(this.instanceMatrix.array,e*16)}getMorphAt(e,t){let i=t.morphTargetInfluences,r=this.morphTexture.source.data.data,s=i.length+1,o=e*s+1;for(let a=0;a<i.length;a++)i[a]=r[o+a]}raycast(e,t){let i=this.matrixWorld,r=this.count;if(Ta.geometry=this.geometry,Ta.material=this.material,Ta.material!==void 0&&(this.boundingSphere===null&&this.computeBoundingSphere(),Da.copy(this.boundingSphere),Da.applyMatrix4(i),e.ray.intersectsSphere(Da)!==!1))for(let s=0;s<r;s++){this.getMatrixAt(s,io),AM.multiplyMatrices(i,io),Ta.matrixWorld=AM,Ta.raycast(e,hu);for(let o=0,a=hu.length;o<a;o++){let l=hu[o];l.instanceId=s,l.object=this,t.push(l)}hu.length=0}}setColorAt(e,t){this.instanceColor===null&&(this.instanceColor=new Uu(new Float32Array(this.instanceMatrix.count*3).fill(1),3)),t.toArray(this.instanceColor.array,e*3)}setMatrixAt(e,t){t.toArray(this.instanceMatrix.array,e*16)}setMorphAt(e,t){let i=t.morphTargetInfluences,r=i.length+1;this.morphTexture===null&&(this.morphTexture=new tg(new Float32Array(r*this.count),r,this.count,Ig,di));let s=this.morphTexture.source.data.data,o=0;for(let c=0;c<i.length;c++)o+=i[c];let a=this.geometry.morphTargetsRelative?1:1-o,l=r*e;s[l]=a,s.set(i,l+1)}updateMorphTargets(){}dispose(){return this.dispatchEvent({type:"dispose"}),this.morphTexture!==null&&(this.morphTexture.dispose(),this.morphTexture=null),this}};var za=class extends zi{constructor(e,t,i,r,s,o,a,l,c){super(e,t,i,r,s,o,a,l,c),this.isCanvasTexture=!0,this.needsUpdate=!0}},Pn=class{constructor(){this.type="Curve",this.arcLengthDivisions=200}getPoint(){return console.warn("THREE.Curve: .getPoint() not implemented."),null}getPointAt(e,t){let i=this.getUtoTmapping(e);return this.getPoint(i,t)}getPoints(e=5){let t=[];for(let i=0;i<=e;i++)t.push(this.getPoint(i/e));return t}getSpacedPoints(e=5){let t=[];for(let i=0;i<=e;i++)t.push(this.getPointAt(i/e));return t}getLength(){let e=this.getLengths();return e[e.length-1]}getLengths(e=this.arcLengthDivisions){if(this.cacheArcLengths&&this.cacheArcLengths.length===e+1&&!this.needsUpdate)return this.cacheArcLengths;this.needsUpdate=!1;let t=[],i,r=this.getPoint(0),s=0;t.push(0);for(let o=1;o<=e;o++)i=this.getPoint(o/e),s+=i.distanceTo(r),t.push(s),r=i;return this.cacheArcLengths=t,t}updateArcLengths(){this.needsUpdate=!0,this.getLengths()}getUtoTmapping(e,t){let i=this.getLengths(),r=0,s=i.length,o;t?o=t:o=e*i[s-1];let a=0,l=s-1,c;for(;a<=l;)if(r=Math.floor(a+(l-a)/2),c=i[r]-o,c<0)a=r+1;else if(c>0)l=r-1;else{l=r;break}if(r=l,i[r]===o)return r/(s-1);let u=i[r],h=i[r+1]-u,f=(o-u)/h;return(r+f)/(s-1)}getTangent(e,t){let r=e-1e-4,s=e+1e-4;r<0&&(r=0),s>1&&(s=1);let o=this.getPoint(r),a=this.getPoint(s),l=t||(o.isVector2?new te:new R);return l.copy(a).sub(o).normalize(),l}getTangentAt(e,t){let i=this.getUtoTmapping(e);return this.getTangent(i,t)}computeFrenetFrames(e,t){let i=new R,r=[],s=[],o=[],a=new R,l=new tt;for(let f=0;f<=e;f++){let m=f/e;r[f]=this.getTangentAt(m,new R)}s[0]=new R,o[0]=new R;let c=Number.MAX_VALUE,u=Math.abs(r[0].x),d=Math.abs(r[0].y),h=Math.abs(r[0].z);u<=c&&(c=u,i.set(1,0,0)),d<=c&&(c=d,i.set(0,1,0)),h<=c&&i.set(0,0,1),a.crossVectors(r[0],i).normalize(),s[0].crossVectors(r[0],a),o[0].crossVectors(r[0],s[0]);for(let f=1;f<=e;f++){if(s[f]=s[f-1].clone(),o[f]=o[f-1].clone(),a.crossVectors(r[f-1],r[f]),a.length()>Number.EPSILON){a.normalize();let m=Math.acos(Bt(r[f-1].dot(r[f]),-1,1));s[f].applyMatrix4(l.makeRotationAxis(a,m))}o[f].crossVectors(r[f],s[f])}if(t===!0){let f=Math.acos(Bt(s[0].dot(s[e]),-1,1));f/=e,r[0].dot(a.crossVectors(s[0],s[e]))>0&&(f=-f);for(let m=1;m<=e;m++)s[m].applyMatrix4(l.makeRotationAxis(r[m],f*m)),o[m].crossVectors(r[m],s[m])}return{tangents:r,normals:s,binormals:o}}clone(){return new this.constructor().copy(this)}copy(e){return this.arcLengthDivisions=e.arcLengthDivisions,this}toJSON(){let e={metadata:{version:4.6,type:"Curve",generator:"Curve.toJSON"}};return e.arcLengthDivisions=this.arcLengthDivisions,e.type=this.type,e}fromJSON(e){return this.arcLengthDivisions=e.arcLengthDivisions,this}},Ga=class extends Pn{constructor(e=0,t=0,i=1,r=1,s=0,o=Math.PI*2,a=!1,l=0){super(),this.isEllipseCurve=!0,this.type="EllipseCurve",this.aX=e,this.aY=t,this.xRadius=i,this.yRadius=r,this.aStartAngle=s,this.aEndAngle=o,this.aClockwise=a,this.aRotation=l}getPoint(e,t=new te){let i=t,r=Math.PI*2,s=this.aEndAngle-this.aStartAngle,o=Math.abs(s)<Number.EPSILON;for(;s<0;)s+=r;for(;s>r;)s-=r;s<Number.EPSILON&&(o?s=0:s=r),this.aClockwise===!0&&!o&&(s===r?s=-r:s=s-r);let a=this.aStartAngle+e*s,l=this.aX+this.xRadius*Math.cos(a),c=this.aY+this.yRadius*Math.sin(a);if(this.aRotation!==0){let u=Math.cos(this.aRotation),d=Math.sin(this.aRotation),h=l-this.aX,f=c-this.aY;l=h*u-f*d+this.aX,c=h*d+f*u+this.aY}return i.set(l,c)}copy(e){return super.copy(e),this.aX=e.aX,this.aY=e.aY,this.xRadius=e.xRadius,this.yRadius=e.yRadius,this.aStartAngle=e.aStartAngle,this.aEndAngle=e.aEndAngle,this.aClockwise=e.aClockwise,this.aRotation=e.aRotation,this}toJSON(){let e=super.toJSON();return e.aX=this.aX,e.aY=this.aY,e.xRadius=this.xRadius,e.yRadius=this.yRadius,e.aStartAngle=this.aStartAngle,e.aEndAngle=this.aEndAngle,e.aClockwise=this.aClockwise,e.aRotation=this.aRotation,e}fromJSON(e){return super.fromJSON(e),this.aX=e.aX,this.aY=e.aY,this.xRadius=e.xRadius,this.yRadius=e.yRadius,this.aStartAngle=e.aStartAngle,this.aEndAngle=e.aEndAngle,this.aClockwise=e.aClockwise,this.aRotation=e.aRotation,this}},ng=class extends Ga{constructor(e,t,i,r,s,o){super(e,t,i,i,r,s,o),this.isArcCurve=!0,this.type="ArcCurve"}};function kg(){let n=0,e=0,t=0,i=0;function r(s,o,a,l){n=s,e=a,t=-3*s+3*o-2*a-l,i=2*s-2*o+a+l}return{initCatmullRom:function(s,o,a,l,c){r(o,a,c*(a-s),c*(l-o))},initNonuniformCatmullRom:function(s,o,a,l,c,u,d){let h=(o-s)/c-(a-s)/(c+u)+(a-o)/u,f=(a-o)/u-(l-o)/(u+d)+(l-a)/d;h*=u,f*=u,r(o,a,h,f)},calc:function(s){let o=s*s,a=o*s;return n+e*s+t*o+i*a}}}var pu=new R,qp=new kg,Xp=new kg,Yp=new kg,ig=class extends Pn{constructor(e=[],t=!1,i="centripetal",r=.5){super(),this.isCatmullRomCurve3=!0,this.type="CatmullRomCurve3",this.points=e,this.closed=t,this.curveType=i,this.tension=r}getPoint(e,t=new R){let i=t,r=this.points,s=r.length,o=(s-(this.closed?0:1))*e,a=Math.floor(o),l=o-a;this.closed?a+=a>0?0:(Math.floor(Math.abs(a)/s)+1)*s:l===0&&a===s-1&&(a=s-2,l=1);let c,u;this.closed||a>0?c=r[(a-1)%s]:(pu.subVectors(r[0],r[1]).add(r[0]),c=pu);let d=r[a%s],h=r[(a+1)%s];if(this.closed||a+2<s?u=r[(a+2)%s]:(pu.subVectors(r[s-1],r[s-2]).add(r[s-1]),u=pu),this.curveType==="centripetal"||this.curveType==="chordal"){let f=this.curveType==="chordal"?.5:.25,m=Math.pow(c.distanceToSquared(d),f),v=Math.pow(d.distanceToSquared(h),f),g=Math.pow(h.distanceToSquared(u),f);v<1e-4&&(v=1),m<1e-4&&(m=v),g<1e-4&&(g=v),qp.initNonuniformCatmullRom(c.x,d.x,h.x,u.x,m,v,g),Xp.initNonuniformCatmullRom(c.y,d.y,h.y,u.y,m,v,g),Yp.initNonuniformCatmullRom(c.z,d.z,h.z,u.z,m,v,g)}else this.curveType==="catmullrom"&&(qp.initCatmullRom(c.x,d.x,h.x,u.x,this.tension),Xp.initCatmullRom(c.y,d.y,h.y,u.y,this.tension),Yp.initCatmullRom(c.z,d.z,h.z,u.z,this.tension));return i.set(qp.calc(l),Xp.calc(l),Yp.calc(l)),i}copy(e){super.copy(e),this.points=[];for(let t=0,i=e.points.length;t<i;t++){let r=e.points[t];this.points.push(r.clone())}return this.closed=e.closed,this.curveType=e.curveType,this.tension=e.tension,this}toJSON(){let e=super.toJSON();e.points=[];for(let t=0,i=this.points.length;t<i;t++){let r=this.points[t];e.points.push(r.toArray())}return e.closed=this.closed,e.curveType=this.curveType,e.tension=this.tension,e}fromJSON(e){super.fromJSON(e),this.points=[];for(let t=0,i=e.points.length;t<i;t++){let r=e.points[t];this.points.push(new R().fromArray(r))}return this.closed=e.closed,this.curveType=e.curveType,this.tension=e.tension,this}};function RM(n,e,t,i,r){let s=(i-e)*.5,o=(r-t)*.5,a=n*n,l=n*a;return(2*t-2*i+s+o)*l+(-3*t+3*i-2*s-o)*a+s*n+t}function VO(n,e){let t=1-n;return t*t*e}function BO(n,e){return 2*(1-n)*n*e}function HO(n,e){return n*n*e}function Oa(n,e,t,i){return VO(n,e)+BO(n,t)+HO(n,i)}function zO(n,e){let t=1-n;return t*t*t*e}function GO(n,e){let t=1-n;return 3*t*t*n*e}function WO(n,e){return 3*(1-n)*n*n*e}function jO(n,e){return n*n*n*e}function La(n,e,t,i,r){return zO(n,e)+GO(n,t)+WO(n,i)+jO(n,r)}var Vu=class extends Pn{constructor(e=new te,t=new te,i=new te,r=new te){super(),this.isCubicBezierCurve=!0,this.type="CubicBezierCurve",this.v0=e,this.v1=t,this.v2=i,this.v3=r}getPoint(e,t=new te){let i=t,r=this.v0,s=this.v1,o=this.v2,a=this.v3;return i.set(La(e,r.x,s.x,o.x,a.x),La(e,r.y,s.y,o.y,a.y)),i}copy(e){return super.copy(e),this.v0.copy(e.v0),this.v1.copy(e.v1),this.v2.copy(e.v2),this.v3.copy(e.v3),this}toJSON(){let e=super.toJSON();return e.v0=this.v0.toArray(),e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e.v3=this.v3.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v0.fromArray(e.v0),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this.v3.fromArray(e.v3),this}},rg=class extends Pn{constructor(e=new R,t=new R,i=new R,r=new R){super(),this.isCubicBezierCurve3=!0,this.type="CubicBezierCurve3",this.v0=e,this.v1=t,this.v2=i,this.v3=r}getPoint(e,t=new R){let i=t,r=this.v0,s=this.v1,o=this.v2,a=this.v3;return i.set(La(e,r.x,s.x,o.x,a.x),La(e,r.y,s.y,o.y,a.y),La(e,r.z,s.z,o.z,a.z)),i}copy(e){return super.copy(e),this.v0.copy(e.v0),this.v1.copy(e.v1),this.v2.copy(e.v2),this.v3.copy(e.v3),this}toJSON(){let e=super.toJSON();return e.v0=this.v0.toArray(),e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e.v3=this.v3.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v0.fromArray(e.v0),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this.v3.fromArray(e.v3),this}},Bu=class extends Pn{constructor(e=new te,t=new te){super(),this.isLineCurve=!0,this.type="LineCurve",this.v1=e,this.v2=t}getPoint(e,t=new te){let i=t;return e===1?i.copy(this.v2):(i.copy(this.v2).sub(this.v1),i.multiplyScalar(e).add(this.v1)),i}getPointAt(e,t){return this.getPoint(e,t)}getTangent(e,t=new te){return t.subVectors(this.v2,this.v1).normalize()}getTangentAt(e,t){return this.getTangent(e,t)}copy(e){return super.copy(e),this.v1.copy(e.v1),this.v2.copy(e.v2),this}toJSON(){let e=super.toJSON();return e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this}},sg=class extends Pn{constructor(e=new R,t=new R){super(),this.isLineCurve3=!0,this.type="LineCurve3",this.v1=e,this.v2=t}getPoint(e,t=new R){let i=t;return e===1?i.copy(this.v2):(i.copy(this.v2).sub(this.v1),i.multiplyScalar(e).add(this.v1)),i}getPointAt(e,t){return this.getPoint(e,t)}getTangent(e,t=new R){return t.subVectors(this.v2,this.v1).normalize()}getTangentAt(e,t){return this.getTangent(e,t)}copy(e){return super.copy(e),this.v1.copy(e.v1),this.v2.copy(e.v2),this}toJSON(){let e=super.toJSON();return e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this}},Hu=class extends Pn{constructor(e=new te,t=new te,i=new te){super(),this.isQuadraticBezierCurve=!0,this.type="QuadraticBezierCurve",this.v0=e,this.v1=t,this.v2=i}getPoint(e,t=new te){let i=t,r=this.v0,s=this.v1,o=this.v2;return i.set(Oa(e,r.x,s.x,o.x),Oa(e,r.y,s.y,o.y)),i}copy(e){return super.copy(e),this.v0.copy(e.v0),this.v1.copy(e.v1),this.v2.copy(e.v2),this}toJSON(){let e=super.toJSON();return e.v0=this.v0.toArray(),e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v0.fromArray(e.v0),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this}},og=class extends Pn{constructor(e=new R,t=new R,i=new R){super(),this.isQuadraticBezierCurve3=!0,this.type="QuadraticBezierCurve3",this.v0=e,this.v1=t,this.v2=i}getPoint(e,t=new R){let i=t,r=this.v0,s=this.v1,o=this.v2;return i.set(Oa(e,r.x,s.x,o.x),Oa(e,r.y,s.y,o.y),Oa(e,r.z,s.z,o.z)),i}copy(e){return super.copy(e),this.v0.copy(e.v0),this.v1.copy(e.v1),this.v2.copy(e.v2),this}toJSON(){let e=super.toJSON();return e.v0=this.v0.toArray(),e.v1=this.v1.toArray(),e.v2=this.v2.toArray(),e}fromJSON(e){return super.fromJSON(e),this.v0.fromArray(e.v0),this.v1.fromArray(e.v1),this.v2.fromArray(e.v2),this}},zu=class extends Pn{constructor(e=[]){super(),this.isSplineCurve=!0,this.type="SplineCurve",this.points=e}getPoint(e,t=new te){let i=t,r=this.points,s=(r.length-1)*e,o=Math.floor(s),a=s-o,l=r[o===0?o:o-1],c=r[o],u=r[o>r.length-2?r.length-1:o+1],d=r[o>r.length-3?r.length-1:o+2];return i.set(RM(a,l.x,c.x,u.x,d.x),RM(a,l.y,c.y,u.y,d.y)),i}copy(e){super.copy(e),this.points=[];for(let t=0,i=e.points.length;t<i;t++){let r=e.points[t];this.points.push(r.clone())}return this}toJSON(){let e=super.toJSON();e.points=[];for(let t=0,i=this.points.length;t<i;t++){let r=this.points[t];e.points.push(r.toArray())}return e}fromJSON(e){super.fromJSON(e),this.points=[];for(let t=0,i=e.points.length;t<i;t++){let r=e.points[t];this.points.push(new te().fromArray(r))}return this}},NM=Object.freeze({__proto__:null,ArcCurve:ng,CatmullRomCurve3:ig,CubicBezierCurve:Vu,CubicBezierCurve3:rg,EllipseCurve:Ga,LineCurve:Bu,LineCurve3:sg,QuadraticBezierCurve:Hu,QuadraticBezierCurve3:og,SplineCurve:zu}),ag=class extends Pn{constructor(){super(),this.type="CurvePath",this.curves=[],this.autoClose=!1}add(e){this.curves.push(e)}closePath(){let e=this.curves[0].getPoint(0),t=this.curves[this.curves.length-1].getPoint(1);if(!e.equals(t)){let i=e.isVector2===!0?"LineCurve":"LineCurve3";this.curves.push(new NM[i](t,e))}return this}getPoint(e,t){let i=e*this.getLength(),r=this.getCurveLengths(),s=0;for(;s<r.length;){if(r[s]>=i){let o=r[s]-i,a=this.curves[s],l=a.getLength(),c=l===0?0:1-o/l;return a.getPointAt(c,t)}s++}return null}getLength(){let e=this.getCurveLengths();return e[e.length-1]}updateArcLengths(){this.needsUpdate=!0,this.cacheLengths=null,this.getCurveLengths()}getCurveLengths(){if(this.cacheLengths&&this.cacheLengths.length===this.curves.length)return this.cacheLengths;let e=[],t=0;for(let i=0,r=this.curves.length;i<r;i++)t+=this.curves[i].getLength(),e.push(t);return this.cacheLengths=e,e}getSpacedPoints(e=40){let t=[];for(let i=0;i<=e;i++)t.push(this.getPoint(i/e));return this.autoClose&&t.push(t[0]),t}getPoints(e=12){let t=[],i;for(let r=0,s=this.curves;r<s.length;r++){let o=s[r],a=o.isEllipseCurve?e*2:o.isLineCurve||o.isLineCurve3?1:o.isSplineCurve?e*o.points.length:e,l=o.getPoints(a);for(let c=0;c<l.length;c++){let u=l[c];i&&i.equals(u)||(t.push(u),i=u)}}return this.autoClose&&t.length>1&&!t[t.length-1].equals(t[0])&&t.push(t[0]),t}copy(e){super.copy(e),this.curves=[];for(let t=0,i=e.curves.length;t<i;t++){let r=e.curves[t];this.curves.push(r.clone())}return this.autoClose=e.autoClose,this}toJSON(){let e=super.toJSON();e.autoClose=this.autoClose,e.curves=[];for(let t=0,i=this.curves.length;t<i;t++){let r=this.curves[t];e.curves.push(r.toJSON())}return e}fromJSON(e){super.fromJSON(e),this.autoClose=e.autoClose,this.curves=[];for(let t=0,i=e.curves.length;t<i;t++){let r=e.curves[t];this.curves.push(new NM[r.type]().fromJSON(r))}return this}},xo=class extends ag{constructor(e){super(),this.type="Path",this.currentPoint=new te,e&&this.setFromPoints(e)}setFromPoints(e){this.moveTo(e[0].x,e[0].y);for(let t=1,i=e.length;t<i;t++)this.lineTo(e[t].x,e[t].y);return this}moveTo(e,t){return this.currentPoint.set(e,t),this}lineTo(e,t){let i=new Bu(this.currentPoint.clone(),new te(e,t));return this.curves.push(i),this.currentPoint.set(e,t),this}quadraticCurveTo(e,t,i,r){let s=new Hu(this.currentPoint.clone(),new te(e,t),new te(i,r));return this.curves.push(s),this.currentPoint.set(i,r),this}bezierCurveTo(e,t,i,r,s,o){let a=new Vu(this.currentPoint.clone(),new te(e,t),new te(i,r),new te(s,o));return this.curves.push(a),this.currentPoint.set(s,o),this}splineThru(e){let t=[this.currentPoint.clone()].concat(e),i=new zu(t);return this.curves.push(i),this.currentPoint.copy(e[e.length-1]),this}arc(e,t,i,r,s,o){let a=this.currentPoint.x,l=this.currentPoint.y;return this.absarc(e+a,t+l,i,r,s,o),this}absarc(e,t,i,r,s,o){return this.absellipse(e,t,i,i,r,s,o),this}ellipse(e,t,i,r,s,o,a,l){let c=this.currentPoint.x,u=this.currentPoint.y;return this.absellipse(e+c,t+u,i,r,s,o,a,l),this}absellipse(e,t,i,r,s,o,a,l){let c=new Ga(e,t,i,r,s,o,a,l);if(this.curves.length>0){let d=c.getPoint(0);d.equals(this.currentPoint)||this.lineTo(d.x,d.y)}this.curves.push(c);let u=c.getPoint(1);return this.currentPoint.copy(u),this}copy(e){return super.copy(e),this.currentPoint.copy(e.currentPoint),this}toJSON(){let e=super.toJSON();return e.currentPoint=this.currentPoint.toArray(),e}fromJSON(e){return super.fromJSON(e),this.currentPoint.fromArray(e.currentPoint),this}};var Gu=class n extends zt{constructor(e=1,t=32,i=0,r=Math.PI*2){super(),this.type="CircleGeometry",this.parameters={radius:e,segments:t,thetaStart:i,thetaLength:r},t=Math.max(3,t);let s=[],o=[],a=[],l=[],c=new R,u=new te;o.push(0,0,0),a.push(0,0,1),l.push(.5,.5);for(let d=0,h=3;d<=t;d++,h+=3){let f=i+d/t*r;c.x=e*Math.cos(f),c.y=e*Math.sin(f),o.push(c.x,c.y,c.z),a.push(0,0,1),u.x=(o[h]/e+1)/2,u.y=(o[h+1]/e+1)/2,l.push(u.x,u.y)}for(let d=1;d<=t;d++)s.push(d,d+1,0);this.setIndex(s),this.setAttribute("position",new dt(o,3)),this.setAttribute("normal",new dt(a,3)),this.setAttribute("uv",new dt(l,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.segments,e.thetaStart,e.thetaLength)}},Zr=class n extends zt{constructor(e=1,t=1,i=1,r=32,s=1,o=!1,a=0,l=Math.PI*2){super(),this.type="CylinderGeometry",this.parameters={radiusTop:e,radiusBottom:t,height:i,radialSegments:r,heightSegments:s,openEnded:o,thetaStart:a,thetaLength:l};let c=this;r=Math.floor(r),s=Math.floor(s);let u=[],d=[],h=[],f=[],m=0,v=[],g=i/2,p=0;M(),o===!1&&(e>0&&_(!0),t>0&&_(!1)),this.setIndex(u),this.setAttribute("position",new dt(d,3)),this.setAttribute("normal",new dt(h,3)),this.setAttribute("uv",new dt(f,2));function M(){let y=new R,A=new R,C=0,S=(t-e)/i;for(let I=0;I<=s;I++){let b=[],x=I/s,D=x*(t-e)+e;for(let N=0;N<=r;N++){let O=N/r,U=O*l+a,$=Math.sin(U),W=Math.cos(U);A.x=D*$,A.y=-x*i+g,A.z=D*W,d.push(A.x,A.y,A.z),y.set($,S,W).normalize(),h.push(y.x,y.y,y.z),f.push(O,1-x),b.push(m++)}v.push(b)}for(let I=0;I<r;I++)for(let b=0;b<s;b++){let x=v[b][I],D=v[b+1][I],N=v[b+1][I+1],O=v[b][I+1];(e>0||b!==0)&&(u.push(x,D,O),C+=3),(t>0||b!==s-1)&&(u.push(D,N,O),C+=3)}c.addGroup(p,C,0),p+=C}function _(y){let A=m,C=new te,S=new R,I=0,b=y===!0?e:t,x=y===!0?1:-1;for(let N=1;N<=r;N++)d.push(0,g*x,0),h.push(0,x,0),f.push(.5,.5),m++;let D=m;for(let N=0;N<=r;N++){let U=N/r*l+a,$=Math.cos(U),W=Math.sin(U);S.x=b*W,S.y=g*x,S.z=b*$,d.push(S.x,S.y,S.z),h.push(0,x,0),C.x=$*.5+.5,C.y=W*.5*x+.5,f.push(C.x,C.y),m++}for(let N=0;N<r;N++){let O=A+N,U=D+N;y===!0?u.push(U,U+1,O):u.push(U+1,U,O),I+=3}c.addGroup(p,I,y===!0?1:2),p+=I}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radiusTop,e.radiusBottom,e.height,e.radialSegments,e.heightSegments,e.openEnded,e.thetaStart,e.thetaLength)}},Mo=class n extends Zr{constructor(e=1,t=1,i=32,r=1,s=!1,o=0,a=Math.PI*2){super(0,e,t,i,r,s,o,a),this.type="ConeGeometry",this.parameters={radius:e,height:t,radialSegments:i,heightSegments:r,openEnded:s,thetaStart:o,thetaLength:a}}static fromJSON(e){return new n(e.radius,e.height,e.radialSegments,e.heightSegments,e.openEnded,e.thetaStart,e.thetaLength)}},Wu=class n extends zt{constructor(e=[],t=[],i=1,r=0){super(),this.type="PolyhedronGeometry",this.parameters={vertices:e,indices:t,radius:i,detail:r};let s=[],o=[];a(r),c(i),u(),this.setAttribute("position",new dt(s,3)),this.setAttribute("normal",new dt(s.slice(),3)),this.setAttribute("uv",new dt(o,2)),r===0?this.computeVertexNormals():this.normalizeNormals();function a(M){let _=new R,y=new R,A=new R;for(let C=0;C<t.length;C+=3)f(t[C+0],_),f(t[C+1],y),f(t[C+2],A),l(_,y,A,M)}function l(M,_,y,A){let C=A+1,S=[];for(let I=0;I<=C;I++){S[I]=[];let b=M.clone().lerp(y,I/C),x=_.clone().lerp(y,I/C),D=C-I;for(let N=0;N<=D;N++)N===0&&I===C?S[I][N]=b:S[I][N]=b.clone().lerp(x,N/D)}for(let I=0;I<C;I++)for(let b=0;b<2*(C-I)-1;b++){let x=Math.floor(b/2);b%2===0?(h(S[I][x+1]),h(S[I+1][x]),h(S[I][x])):(h(S[I][x+1]),h(S[I+1][x+1]),h(S[I+1][x]))}}function c(M){let _=new R;for(let y=0;y<s.length;y+=3)_.x=s[y+0],_.y=s[y+1],_.z=s[y+2],_.normalize().multiplyScalar(M),s[y+0]=_.x,s[y+1]=_.y,s[y+2]=_.z}function u(){let M=new R;for(let _=0;_<s.length;_+=3){M.x=s[_+0],M.y=s[_+1],M.z=s[_+2];let y=g(M)/2/Math.PI+.5,A=p(M)/Math.PI+.5;o.push(y,1-A)}m(),d()}function d(){for(let M=0;M<o.length;M+=6){let _=o[M+0],y=o[M+2],A=o[M+4],C=Math.max(_,y,A),S=Math.min(_,y,A);C>.9&&S<.1&&(_<.2&&(o[M+0]+=1),y<.2&&(o[M+2]+=1),A<.2&&(o[M+4]+=1))}}function h(M){s.push(M.x,M.y,M.z)}function f(M,_){let y=M*3;_.x=e[y+0],_.y=e[y+1],_.z=e[y+2]}function m(){let M=new R,_=new R,y=new R,A=new R,C=new te,S=new te,I=new te;for(let b=0,x=0;b<s.length;b+=9,x+=6){M.set(s[b+0],s[b+1],s[b+2]),_.set(s[b+3],s[b+4],s[b+5]),y.set(s[b+6],s[b+7],s[b+8]),C.set(o[x+0],o[x+1]),S.set(o[x+2],o[x+3]),I.set(o[x+4],o[x+5]),A.copy(M).add(_).add(y).divideScalar(3);let D=g(A);v(C,x+0,M,D),v(S,x+2,_,D),v(I,x+4,y,D)}}function v(M,_,y,A){A<0&&M.x===1&&(o[_]=M.x-1),y.x===0&&y.z===0&&(o[_]=A/2/Math.PI+.5)}function g(M){return Math.atan2(M.z,-M.x)}function p(M){return Math.atan2(-M.y,Math.sqrt(M.x*M.x+M.z*M.z))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.vertices,e.indices,e.radius,e.details)}},ju=class n extends Wu{constructor(e=1,t=0){let i=(1+Math.sqrt(5))/2,r=1/i,s=[-1,-1,-1,-1,-1,1,-1,1,-1,-1,1,1,1,-1,-1,1,-1,1,1,1,-1,1,1,1,0,-r,-i,0,-r,i,0,r,-i,0,r,i,-r,-i,0,-r,i,0,r,-i,0,r,i,0,-i,0,-r,i,0,-r,-i,0,r,i,0,r],o=[3,11,7,3,7,15,3,15,13,7,19,17,7,17,6,7,6,15,17,4,8,17,8,10,17,10,6,8,0,16,8,16,2,8,2,10,0,12,1,0,1,18,0,18,16,6,10,2,6,2,13,6,13,15,2,16,18,2,18,3,2,3,13,18,1,9,18,9,11,18,11,3,4,14,12,4,12,0,4,0,8,11,9,5,11,5,19,11,19,7,19,5,14,19,14,4,19,4,17,1,12,14,1,14,5,1,5,9];super(s,o,e,t),this.type="DodecahedronGeometry",this.parameters={radius:e,detail:t}}static fromJSON(e){return new n(e.radius,e.detail)}};var Wa=class extends xo{constructor(e){super(e),this.uuid=mi(),this.type="Shape",this.holes=[]}getPointsHoles(e){let t=[];for(let i=0,r=this.holes.length;i<r;i++)t[i]=this.holes[i].getPoints(e);return t}extractPoints(e){return{shape:this.getPoints(e),holes:this.getPointsHoles(e)}}copy(e){super.copy(e),this.holes=[];for(let t=0,i=e.holes.length;t<i;t++){let r=e.holes[t];this.holes.push(r.clone())}return this}toJSON(){let e=super.toJSON();e.uuid=this.uuid,e.holes=[];for(let t=0,i=this.holes.length;t<i;t++){let r=this.holes[t];e.holes.push(r.toJSON())}return e}fromJSON(e){super.fromJSON(e),this.uuid=e.uuid,this.holes=[];for(let t=0,i=e.holes.length;t<i;t++){let r=e.holes[t];this.holes.push(new xo().fromJSON(r))}return this}},$O={triangulate:function(n,e,t=2){let i=e&&e.length,r=i?e[0]*t:n.length,s=aE(n,0,r,t,!0),o=[];if(!s||s.next===s.prev)return o;let a,l,c,u,d,h,f;if(i&&(s=KO(n,e,s,t)),n.length>80*t){a=c=n[0],l=u=n[1];for(let m=t;m<r;m+=t)d=n[m],h=n[m+1],d<a&&(a=d),h<l&&(l=h),d>c&&(c=d),h>u&&(u=h);f=Math.max(c-a,u-l),f=f!==0?32767/f:0}return ja(s,o,t,a,l,f,0),o}};function aE(n,e,t,i,r){let s,o;if(r===lL(n,e,t,i)>0)for(s=e;s<t;s+=i)o=PM(s,n[s],n[s+1],o);else for(s=t-i;s>=e;s-=i)o=PM(s,n[s],n[s+1],o);return o&&id(o,o.next)&&(qa(o),o=o.next),o}function Kr(n,e){if(!n)return n;e||(e=n);let t=n,i;do if(i=!1,!t.steiner&&(id(t,t.next)||Ct(t.prev,t,t.next)===0)){if(qa(t),t=e=t.prev,t===t.next)break;i=!0}else t=t.next;while(i||t!==e);return e}function ja(n,e,t,i,r,s,o){if(!n)return;!o&&s&&nL(n,i,r,s);let a=n,l,c;for(;n.prev!==n.next;){if(l=n.prev,c=n.next,s?XO(n,i,r,s):qO(n)){e.push(l.i/t|0),e.push(n.i/t|0),e.push(c.i/t|0),qa(n),n=c.next,a=c.next;continue}if(n=c,n===a){o?o===1?(n=YO(Kr(n),e,t),ja(n,e,t,i,r,s,2)):o===2&&ZO(n,e,t,i,r,s):ja(Kr(n),e,t,i,r,s,1);break}}}function qO(n){let e=n.prev,t=n,i=n.next;if(Ct(e,t,i)>=0)return!1;let r=e.x,s=t.x,o=i.x,a=e.y,l=t.y,c=i.y,u=r<s?r<o?r:o:s<o?s:o,d=a<l?a<c?a:c:l<c?l:c,h=r>s?r>o?r:o:s>o?s:o,f=a>l?a>c?a:c:l>c?l:c,m=i.next;for(;m!==e;){if(m.x>=u&&m.x<=h&&m.y>=d&&m.y<=f&&so(r,a,s,l,o,c,m.x,m.y)&&Ct(m.prev,m,m.next)>=0)return!1;m=m.next}return!0}function XO(n,e,t,i){let r=n.prev,s=n,o=n.next;if(Ct(r,s,o)>=0)return!1;let a=r.x,l=s.x,c=o.x,u=r.y,d=s.y,h=o.y,f=a<l?a<c?a:c:l<c?l:c,m=u<d?u<h?u:h:d<h?d:h,v=a>l?a>c?a:c:l>c?l:c,g=u>d?u>h?u:h:d>h?d:h,p=lg(f,m,e,t,i),M=lg(v,g,e,t,i),_=n.prevZ,y=n.nextZ;for(;_&&_.z>=p&&y&&y.z<=M;){if(_.x>=f&&_.x<=v&&_.y>=m&&_.y<=g&&_!==r&&_!==o&&so(a,u,l,d,c,h,_.x,_.y)&&Ct(_.prev,_,_.next)>=0||(_=_.prevZ,y.x>=f&&y.x<=v&&y.y>=m&&y.y<=g&&y!==r&&y!==o&&so(a,u,l,d,c,h,y.x,y.y)&&Ct(y.prev,y,y.next)>=0))return!1;y=y.nextZ}for(;_&&_.z>=p;){if(_.x>=f&&_.x<=v&&_.y>=m&&_.y<=g&&_!==r&&_!==o&&so(a,u,l,d,c,h,_.x,_.y)&&Ct(_.prev,_,_.next)>=0)return!1;_=_.prevZ}for(;y&&y.z<=M;){if(y.x>=f&&y.x<=v&&y.y>=m&&y.y<=g&&y!==r&&y!==o&&so(a,u,l,d,c,h,y.x,y.y)&&Ct(y.prev,y,y.next)>=0)return!1;y=y.nextZ}return!0}function YO(n,e,t){let i=n;do{let r=i.prev,s=i.next.next;!id(r,s)&&lE(r,i,i.next,s)&&$a(r,s)&&$a(s,r)&&(e.push(r.i/t|0),e.push(i.i/t|0),e.push(s.i/t|0),qa(i),qa(i.next),i=n=s),i=i.next}while(i!==n);return Kr(i)}function ZO(n,e,t,i,r,s){let o=n;do{let a=o.next.next;for(;a!==o.prev;){if(o.i!==a.i&&sL(o,a)){let l=cE(o,a);o=Kr(o,o.next),l=Kr(l,l.next),ja(o,e,t,i,r,s,0),ja(l,e,t,i,r,s,0);return}a=a.next}o=o.next}while(o!==n)}function KO(n,e,t,i){let r=[],s,o,a,l,c;for(s=0,o=e.length;s<o;s++)a=e[s]*i,l=s<o-1?e[s+1]*i:n.length,c=aE(n,a,l,i,!1),c===c.next&&(c.steiner=!0),r.push(rL(c));for(r.sort(JO),s=0;s<r.length;s++)t=QO(r[s],t);return t}function JO(n,e){return n.x-e.x}function QO(n,e){let t=eL(n,e);if(!t)return e;let i=cE(t,n);return Kr(i,i.next),Kr(t,t.next)}function eL(n,e){let t=e,i=-1/0,r,s=n.x,o=n.y;do{if(o<=t.y&&o>=t.next.y&&t.next.y!==t.y){let h=t.x+(o-t.y)*(t.next.x-t.x)/(t.next.y-t.y);if(h<=s&&h>i&&(i=h,r=t.x<t.next.x?t:t.next,h===s))return r}t=t.next}while(t!==e);if(!r)return null;let a=r,l=r.x,c=r.y,u=1/0,d;t=r;do s>=t.x&&t.x>=l&&s!==t.x&&so(o<c?s:i,o,l,c,o<c?i:s,o,t.x,t.y)&&(d=Math.abs(o-t.y)/(s-t.x),$a(t,n)&&(d<u||d===u&&(t.x>r.x||t.x===r.x&&tL(r,t)))&&(r=t,u=d)),t=t.next;while(t!==a);return r}function tL(n,e){return Ct(n.prev,n,e.prev)<0&&Ct(e.next,n,n.next)<0}function nL(n,e,t,i){let r=n;do r.z===0&&(r.z=lg(r.x,r.y,e,t,i)),r.prevZ=r.prev,r.nextZ=r.next,r=r.next;while(r!==n);r.prevZ.nextZ=null,r.prevZ=null,iL(r)}function iL(n){let e,t,i,r,s,o,a,l,c=1;do{for(t=n,n=null,s=null,o=0;t;){for(o++,i=t,a=0,e=0;e<c&&(a++,i=i.nextZ,!!i);e++);for(l=c;a>0||l>0&&i;)a!==0&&(l===0||!i||t.z<=i.z)?(r=t,t=t.nextZ,a--):(r=i,i=i.nextZ,l--),s?s.nextZ=r:n=r,r.prevZ=s,s=r;t=i}s.nextZ=null,c*=2}while(o>1);return n}function lg(n,e,t,i,r){return n=(n-t)*r|0,e=(e-i)*r|0,n=(n|n<<8)&16711935,n=(n|n<<4)&252645135,n=(n|n<<2)&858993459,n=(n|n<<1)&1431655765,e=(e|e<<8)&16711935,e=(e|e<<4)&252645135,e=(e|e<<2)&858993459,e=(e|e<<1)&1431655765,n|e<<1}function rL(n){let e=n,t=n;do(e.x<t.x||e.x===t.x&&e.y<t.y)&&(t=e),e=e.next;while(e!==n);return t}function so(n,e,t,i,r,s,o,a){return(r-o)*(e-a)>=(n-o)*(s-a)&&(n-o)*(i-a)>=(t-o)*(e-a)&&(t-o)*(s-a)>=(r-o)*(i-a)}function sL(n,e){return n.next.i!==e.i&&n.prev.i!==e.i&&!oL(n,e)&&($a(n,e)&&$a(e,n)&&aL(n,e)&&(Ct(n.prev,n,e.prev)||Ct(n,e.prev,e))||id(n,e)&&Ct(n.prev,n,n.next)>0&&Ct(e.prev,e,e.next)>0)}function Ct(n,e,t){return(e.y-n.y)*(t.x-e.x)-(e.x-n.x)*(t.y-e.y)}function id(n,e){return n.x===e.x&&n.y===e.y}function lE(n,e,t,i){let r=gu(Ct(n,e,t)),s=gu(Ct(n,e,i)),o=gu(Ct(t,i,n)),a=gu(Ct(t,i,e));return!!(r!==s&&o!==a||r===0&&mu(n,t,e)||s===0&&mu(n,i,e)||o===0&&mu(t,n,i)||a===0&&mu(t,e,i))}function mu(n,e,t){return e.x<=Math.max(n.x,t.x)&&e.x>=Math.min(n.x,t.x)&&e.y<=Math.max(n.y,t.y)&&e.y>=Math.min(n.y,t.y)}function gu(n){return n>0?1:n<0?-1:0}function oL(n,e){let t=n;do{if(t.i!==n.i&&t.next.i!==n.i&&t.i!==e.i&&t.next.i!==e.i&&lE(t,t.next,n,e))return!0;t=t.next}while(t!==n);return!1}function $a(n,e){return Ct(n.prev,n,n.next)<0?Ct(n,e,n.next)>=0&&Ct(n,n.prev,e)>=0:Ct(n,e,n.prev)<0||Ct(n,n.next,e)<0}function aL(n,e){let t=n,i=!1,r=(n.x+e.x)/2,s=(n.y+e.y)/2;do t.y>s!=t.next.y>s&&t.next.y!==t.y&&r<(t.next.x-t.x)*(s-t.y)/(t.next.y-t.y)+t.x&&(i=!i),t=t.next;while(t!==n);return i}function cE(n,e){let t=new cg(n.i,n.x,n.y),i=new cg(e.i,e.x,e.y),r=n.next,s=e.prev;return n.next=e,e.prev=n,t.next=r,r.prev=t,i.next=t,t.prev=i,s.next=i,i.prev=s,i}function PM(n,e,t,i){let r=new cg(n,e,t);return i?(r.next=i.next,r.prev=i,i.next.prev=r,i.next=r):(r.prev=r,r.next=r),r}function qa(n){n.next.prev=n.prev,n.prev.next=n.next,n.prevZ&&(n.prevZ.nextZ=n.nextZ),n.nextZ&&(n.nextZ.prevZ=n.prevZ)}function cg(n,e,t){this.i=n,this.x=e,this.y=t,this.prev=null,this.next=null,this.z=0,this.prevZ=null,this.nextZ=null,this.steiner=!1}function lL(n,e,t,i){let r=0;for(let s=e,o=t-i;s<t;s+=i)r+=(n[o]-n[s])*(n[s+1]+n[o+1]),o=s;return r}var Fa=class n{static area(e){let t=e.length,i=0;for(let r=t-1,s=0;s<t;r=s++)i+=e[r].x*e[s].y-e[s].x*e[r].y;return i*.5}static isClockWise(e){return n.area(e)<0}static triangulateShape(e,t){let i=[],r=[],s=[];OM(e),LM(i,e);let o=e.length;t.forEach(OM);for(let l=0;l<t.length;l++)r.push(o),o+=t[l].length,LM(i,t[l]);let a=$O.triangulate(i,r);for(let l=0;l<a.length;l+=3)s.push(a.slice(l,l+3));return s}};function OM(n){let e=n.length;e>2&&n[e-1].equals(n[0])&&n.pop()}function LM(n,e){for(let t=0;t<e.length;t++)n.push(e[t].x),n.push(e[t].y)}var Eo=class n extends Wu{constructor(e=1,t=0){let i=(1+Math.sqrt(5))/2,r=[-1,i,0,1,i,0,-1,-i,0,1,-i,0,0,-1,i,0,1,i,0,-1,-i,0,1,-i,i,0,-1,i,0,1,-i,0,-1,-i,0,1],s=[0,11,5,0,5,1,0,1,7,0,7,10,0,10,11,1,5,9,5,11,4,11,10,2,10,7,6,7,1,8,3,9,4,3,4,2,3,2,6,3,6,8,3,8,9,4,9,5,2,4,11,6,2,10,8,6,7,9,8,1];super(r,s,e,t),this.type="IcosahedronGeometry",this.parameters={radius:e,detail:t}}static fromJSON(e){return new n(e.radius,e.detail)}};var $u=class n extends zt{constructor(e=new Wa([new te(0,.5),new te(-.5,-.5),new te(.5,-.5)]),t=12){super(),this.type="ShapeGeometry",this.parameters={shapes:e,curveSegments:t};let i=[],r=[],s=[],o=[],a=0,l=0;if(Array.isArray(e)===!1)c(e);else for(let u=0;u<e.length;u++)c(e[u]),this.addGroup(a,l,u),a+=l,l=0;this.setIndex(i),this.setAttribute("position",new dt(r,3)),this.setAttribute("normal",new dt(s,3)),this.setAttribute("uv",new dt(o,2));function c(u){let d=r.length/3,h=u.extractPoints(t),f=h.shape,m=h.holes;Fa.isClockWise(f)===!1&&(f=f.reverse());for(let g=0,p=m.length;g<p;g++){let M=m[g];Fa.isClockWise(M)===!0&&(m[g]=M.reverse())}let v=Fa.triangulateShape(f,m);for(let g=0,p=m.length;g<p;g++){let M=m[g];f=f.concat(M)}for(let g=0,p=f.length;g<p;g++){let M=f[g];r.push(M.x,M.y,0),s.push(0,0,1),o.push(M.x,M.y)}for(let g=0,p=v.length;g<p;g++){let M=v[g],_=M[0]+d,y=M[1]+d,A=M[2]+d;i.push(_,y,A),l+=3}}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}toJSON(){let e=super.toJSON(),t=this.parameters.shapes;return cL(t,e)}static fromJSON(e,t){let i=[];for(let r=0,s=e.shapes.length;r<s;r++){let o=t[e.shapes[r]];i.push(o)}return new n(i,e.curveSegments)}};function cL(n,e){if(e.shapes=[],Array.isArray(n))for(let t=0,i=n.length;t<i;t++){let r=n[t];e.shapes.push(r.uuid)}else e.shapes.push(n.uuid);return e}var Xa=class n extends zt{constructor(e=1,t=32,i=16,r=0,s=Math.PI*2,o=0,a=Math.PI){super(),this.type="SphereGeometry",this.parameters={radius:e,widthSegments:t,heightSegments:i,phiStart:r,phiLength:s,thetaStart:o,thetaLength:a},t=Math.max(3,Math.floor(t)),i=Math.max(2,Math.floor(i));let l=Math.min(o+a,Math.PI),c=0,u=[],d=new R,h=new R,f=[],m=[],v=[],g=[];for(let p=0;p<=i;p++){let M=[],_=p/i,y=0;p===0&&o===0?y=.5/t:p===i&&l===Math.PI&&(y=-.5/t);for(let A=0;A<=t;A++){let C=A/t;d.x=-e*Math.cos(r+C*s)*Math.sin(o+_*a),d.y=e*Math.cos(o+_*a),d.z=e*Math.sin(r+C*s)*Math.sin(o+_*a),m.push(d.x,d.y,d.z),h.copy(d).normalize(),v.push(h.x,h.y,h.z),g.push(C+y,1-_),M.push(c++)}u.push(M)}for(let p=0;p<i;p++)for(let M=0;M<t;M++){let _=u[p][M+1],y=u[p][M],A=u[p+1][M],C=u[p+1][M+1];(p!==0||o>0)&&f.push(_,y,C),(p!==i-1||l<Math.PI)&&f.push(y,A,C)}this.setIndex(f),this.setAttribute("position",new dt(m,3)),this.setAttribute("normal",new dt(v,3)),this.setAttribute("uv",new dt(g,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.widthSegments,e.heightSegments,e.phiStart,e.phiLength,e.thetaStart,e.thetaLength)}};var qu=class extends Gt{static get type(){return"RawShaderMaterial"}constructor(e){super(e),this.isRawShaderMaterial=!0}};var Xu=class extends Hi{static get type(){return"MeshPhongMaterial"}constructor(e){super(),this.isMeshPhongMaterial=!0,this.color=new xe(16777215),this.specular=new xe(1118481),this.shininess=30,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new xe(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=Og,this.normalScale=new te(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new $r,this.combine=xg,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.specular.copy(e.specular),this.shininess=e.shininess,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}},Kt=class extends Hi{static get type(){return"MeshToonMaterial"}constructor(e){super(),this.isMeshToonMaterial=!0,this.defines={TOON:""},this.color=new xe(16777215),this.map=null,this.gradientMap=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new xe(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=Og,this.normalScale=new te(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.alphaMap=null,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.gradientMap=e.gradientMap,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.alphaMap=e.alphaMap,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}};function yu(n,e,t){return!n||!t&&n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}function uL(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}var bo=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let l=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===l)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},ug=class extends bo{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:Rx,endingEnd:Rx}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],l=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case Nx:s=e,a=2*t-i;break;case Px:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(l===void 0)switch(this.getSettings_().endingEnd){case Nx:o=e,l=2*i-t;break;case Px:o=1,l=i+r[1]-r[0];break;default:o=e-1,l=t}let c=(i-t)*.5,u=this.valueSize;this._weightPrev=c/(t-a),this._weightNext=c/(l-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=this._offsetPrev,d=this._offsetNext,h=this._weightPrev,f=this._weightNext,m=(i-t)/(r-t),v=m*m,g=v*m,p=-h*g+2*h*v-h*m,M=(1+h)*g+(-1.5-2*h)*v+(-.5+h)*m+1,_=(-1-f)*g+(1.5+f)*v+.5*m,y=f*g-f*v;for(let A=0;A!==a;++A)s[A]=p*o[u+A]+M*o[c+A]+_*o[l+A]+y*o[d+A];return s}},dg=class extends bo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=e*a,c=l-a,u=(i-t)/(r-t),d=1-u;for(let h=0;h!==a;++h)s[h]=o[c+h]*d+o[l+h]*u;return s}},fg=class extends bo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Xn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=yu(t,this.TimeBufferType),this.values=yu(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:yu(e.times,Array),values:yu(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new fg(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new dg(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new ug(this.times,this.values,this.getValueSize(),e)}setInterpolation(e){let t;switch(e){case bu:t=this.InterpolantFactoryMethodDiscrete;break;case Fm:t=this.InterpolantFactoryMethodLinear;break;case _p:t=this.InterpolantFactoryMethodSmooth;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return console.warn("THREE.KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return bu;case this.InterpolantFactoryMethodLinear:return Fm;case this.InterpolantFactoryMethodSmooth:return _p}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(console.error("THREE.KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(console.error("THREE.KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let l=i[a];if(typeof l=="number"&&isNaN(l)){console.error("THREE.KeyframeTrack: Time is not a valid number.",this,a,l),e=!1;break}if(o!==null&&o>l){console.error("THREE.KeyframeTrack: Out of order keys.",this,a,l,o),e=!1;break}o=l}if(r!==void 0&&uL(r))for(let a=0,l=r.length;a!==l;++a){let c=r[a];if(isNaN(c)){console.error("THREE.KeyframeTrack: Value is not a valid number.",this,a,c),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===_p,s=e.length-1,o=1;for(let a=1;a<s;++a){let l=!1,c=e[a],u=e[a+1];if(c!==u&&(a!==1||c!==e[0]))if(r)l=!0;else{let d=a*i,h=d-i,f=d+i;for(let m=0;m!==i;++m){let v=t[d+m];if(v!==t[h+m]||v!==t[f+m]){l=!0;break}}}if(l){if(a!==o){e[o]=e[a];let d=a*i,h=o*i;for(let f=0;f!==i;++f)t[h+f]=t[d+f]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,l=o*i,c=0;c!==i;++c)t[l+c]=t[a+c];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};Xn.prototype.TimeBufferType=Float32Array;Xn.prototype.ValueBufferType=Float32Array;Xn.prototype.DefaultInterpolation=Fm;var Jr=class extends Xn{constructor(e,t,i){super(e,t,i)}};Jr.prototype.ValueTypeName="bool";Jr.prototype.ValueBufferType=Array;Jr.prototype.DefaultInterpolation=bu;Jr.prototype.InterpolantFactoryMethodLinear=void 0;Jr.prototype.InterpolantFactoryMethodSmooth=void 0;var hg=class extends Xn{};hg.prototype.ValueTypeName="color";var pg=class extends Xn{};pg.prototype.ValueTypeName="number";var mg=class extends bo{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,l=(i-t)/(r-t),c=e*a;for(let u=c+a;c!==u;c+=4)Pt.slerpFlat(s,0,o,c-a,o,c,l);return s}},Yu=class extends Xn{InterpolantFactoryMethodLinear(e){return new mg(this.times,this.values,this.getValueSize(),e)}};Yu.prototype.ValueTypeName="quaternion";Yu.prototype.InterpolantFactoryMethodSmooth=void 0;var Qr=class extends Xn{constructor(e,t,i){super(e,t,i)}};Qr.prototype.ValueTypeName="string";Qr.prototype.ValueBufferType=Array;Qr.prototype.DefaultInterpolation=bu;Qr.prototype.InterpolantFactoryMethodLinear=void 0;Qr.prototype.InterpolantFactoryMethodSmooth=void 0;var gg=class extends Xn{};gg.prototype.ValueTypeName="vector";var Zu=class extends qn{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new xe(e),this.intensity=t}dispose(){}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,this.groundColor!==void 0&&(t.object.groundColor=this.groundColor.getHex()),this.distance!==void 0&&(t.object.distance=this.distance),this.angle!==void 0&&(t.object.angle=this.angle),this.decay!==void 0&&(t.object.decay=this.decay),this.penumbra!==void 0&&(t.object.penumbra=this.penumbra),this.shadow!==void 0&&(t.object.shadow=this.shadow.toJSON()),this.target!==void 0&&(t.object.target=this.target.uuid),t}},Ku=class extends Zu{constructor(e,t,i){super(e,i),this.isHemisphereLight=!0,this.type="HemisphereLight",this.position.copy(qn.DEFAULT_UP),this.updateMatrix(),this.groundColor=new xe(t)}copy(e,t){return super.copy(e,t),this.groundColor.copy(e.groundColor),this}},Zp=new tt,FM=new R,kM=new R,yg=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new te(512,512),this.map=null,this.mapPass=null,this.matrix=new tt,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new Ba,this._frameExtents=new te(1,1),this._viewportCount=1,this._viewports=[new At(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;FM.setFromMatrixPosition(e.matrixWorld),t.position.copy(FM),kM.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(kM),t.updateMatrixWorld(),Zp.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(Zp),i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(Zp)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.mapSize.copy(e.mapSize),this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}};var vg=class extends yg{constructor(){super(new vo(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},Ju=class extends Zu{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(qn.DEFAULT_UP),this.updateMatrix(),this.target=new qn,this.shadow=new vg}dispose(){this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}};var wo=class{constructor(e=!0){this.autoStart=e,this.startTime=0,this.oldTime=0,this.elapsedTime=0,this.running=!1}start(){this.startTime=UM(),this.oldTime=this.startTime,this.elapsedTime=0,this.running=!0}stop(){this.getElapsedTime(),this.running=!1,this.autoStart=!1}getElapsedTime(){return this.getDelta(),this.elapsedTime}getDelta(){let e=0;if(this.autoStart&&!this.running)return this.start(),0;if(this.running){let t=UM();e=(t-this.oldTime)/1e3,this.oldTime=t,this.elapsedTime+=e}return e}};function UM(){return performance.now()}var Ug="\\[\\]\\.:\\/",dL=new RegExp("["+Ug+"]","g"),Vg="[^"+Ug+"]",fL="[^"+Ug.replace("\\.","")+"]",hL=/((?:WC+[\/:])*)/.source.replace("WC",Vg),pL=/(WCOD+)?/.source.replace("WCOD",fL),mL=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",Vg),gL=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",Vg),yL=new RegExp("^"+hL+pL+mL+gL+"$"),vL=["material","materials","bones","map"],_g=class{constructor(e,t,i){let r=i||It.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},It=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(dL,"")}static parseTrackName(t){let i=yL.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);vL.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let l=o[a];if(l.name===i||l.uuid===i)return l;let c=r(l.children);if(c)return c}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){console.warn("THREE.PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){console.error("THREE.PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){console.error("THREE.PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){console.error("THREE.PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){console.error("THREE.PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){console.error("THREE.PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){console.error("THREE.PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;console.error("THREE.PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let l=this.Versioning.None;this.targetObject=t,t.needsUpdate!==void 0?l=this.Versioning.NeedsUpdate:t.matrixWorldNeedsUpdate!==void 0&&(l=this.Versioning.MatrixWorldNeedsUpdate);let c=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){console.error("THREE.PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}c=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(c=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(c=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[c],this.setValue=this.SetterByBindingTypeAndVersioning[c][l]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=_g,n})();It.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};It.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};It.prototype.GetterByBindingType=[It.prototype._getValue_direct,It.prototype._getValue_array,It.prototype._getValue_arrayElement,It.prototype._getValue_toArray];It.prototype.SetterByBindingTypeAndVersioning=[[It.prototype._setValue_direct,It.prototype._setValue_direct_setNeedsUpdate,It.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[It.prototype._setValue_array,It.prototype._setValue_array_setNeedsUpdate,It.prototype._setValue_array_setMatrixWorldNeedsUpdate],[It.prototype._setValue_arrayElement,It.prototype._setValue_arrayElement_setNeedsUpdate,It.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[It.prototype._setValue_fromArray,It.prototype._setValue_fromArray_setNeedsUpdate,It.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var ZV=new Float32Array(1);var Ya=class{constructor(e=1,t=0,i=0){return this.radius=e,this.phi=t,this.theta=i,this}set(e,t,i){return this.radius=e,this.phi=t,this.theta=i,this}copy(e){return this.radius=e.radius,this.phi=e.phi,this.theta=e.theta,this}makeSafe(){return this.phi=Math.max(1e-6,Math.min(Math.PI-1e-6,this.phi)),this}setFromVector3(e){return this.setFromCartesianCoords(e.x,e.y,e.z)}setFromCartesianCoords(e,t,i){return this.radius=Math.sqrt(e*e+t*t+i*i),this.radius===0?(this.theta=0,this.phi=0):(this.theta=Math.atan2(e,i),this.phi=Math.acos(Bt(t/this.radius,-1,1))),this}clone(){return new this.constructor().copy(this)}};var Qu=class extends Vi{constructor(e,t=null){super(),this.object=e,this.domElement=t,this.enabled=!0,this.state=-1,this.keys={},this.mouseButtons={LEFT:null,MIDDLE:null,RIGHT:null},this.touches={ONE:null,TWO:null}}connect(){}disconnect(){}dispose(){}update(){}};typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"170"}}));typeof window<"u"&&(window.__THREE__?console.warn("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="170");var uE={type:"change"},Hg={type:"start"},fE={type:"end"},rd=new Va,dE=new Wn,xL=Math.cos(70*KM.DEG2RAD),Ft=new R,dn=2*Math.PI,ht={NONE:-1,ROTATE:0,DOLLY:1,PAN:2,TOUCH_ROTATE:3,TOUCH_PAN:4,TOUCH_DOLLY_PAN:5,TOUCH_DOLLY_ROTATE:6},Bg=1e-6,sd=class extends Qu{constructor(e,t=null){super(e,t),this.state=ht.NONE,this.enabled=!0,this.target=new R,this.cursor=new R,this.minDistance=0,this.maxDistance=1/0,this.minZoom=0,this.maxZoom=1/0,this.minTargetRadius=0,this.maxTargetRadius=1/0,this.minPolarAngle=0,this.maxPolarAngle=Math.PI,this.minAzimuthAngle=-1/0,this.maxAzimuthAngle=1/0,this.enableDamping=!1,this.dampingFactor=.05,this.enableZoom=!0,this.zoomSpeed=1,this.enableRotate=!0,this.rotateSpeed=1,this.enablePan=!0,this.panSpeed=1,this.screenSpacePanning=!0,this.keyPanSpeed=7,this.zoomToCursor=!1,this.autoRotate=!1,this.autoRotateSpeed=2,this.keys={LEFT:"ArrowLeft",UP:"ArrowUp",RIGHT:"ArrowRight",BOTTOM:"ArrowDown"},this.mouseButtons={LEFT:es.ROTATE,MIDDLE:es.DOLLY,RIGHT:es.PAN},this.touches={ONE:ts.ROTATE,TWO:ts.DOLLY_PAN},this.target0=this.target.clone(),this.position0=this.object.position.clone(),this.zoom0=this.object.zoom,this._domElementKeyEvents=null,this._lastPosition=new R,this._lastQuaternion=new Pt,this._lastTargetPosition=new R,this._quat=new Pt().setFromUnitVectors(e.up,new R(0,1,0)),this._quatInverse=this._quat.clone().invert(),this._spherical=new Ya,this._sphericalDelta=new Ya,this._scale=1,this._panOffset=new R,this._rotateStart=new te,this._rotateEnd=new te,this._rotateDelta=new te,this._panStart=new te,this._panEnd=new te,this._panDelta=new te,this._dollyStart=new te,this._dollyEnd=new te,this._dollyDelta=new te,this._dollyDirection=new R,this._mouse=new te,this._performCursorZoom=!1,this._pointers=[],this._pointerPositions={},this._controlActive=!1,this._onPointerMove=EL.bind(this),this._onPointerDown=ML.bind(this),this._onPointerUp=bL.bind(this),this._onContextMenu=IL.bind(this),this._onMouseWheel=CL.bind(this),this._onKeyDown=TL.bind(this),this._onTouchStart=DL.bind(this),this._onTouchMove=AL.bind(this),this._onMouseDown=wL.bind(this),this._onMouseMove=SL.bind(this),this._interceptControlDown=RL.bind(this),this._interceptControlUp=NL.bind(this),this.domElement!==null&&this.connect(),this.update()}connect(){this.domElement.addEventListener("pointerdown",this._onPointerDown),this.domElement.addEventListener("pointercancel",this._onPointerUp),this.domElement.addEventListener("contextmenu",this._onContextMenu),this.domElement.addEventListener("wheel",this._onMouseWheel,{passive:!1}),this.domElement.getRootNode().addEventListener("keydown",this._interceptControlDown,{passive:!0,capture:!0}),this.domElement.style.touchAction="none"}disconnect(){this.domElement.removeEventListener("pointerdown",this._onPointerDown),this.domElement.removeEventListener("pointermove",this._onPointerMove),this.domElement.removeEventListener("pointerup",this._onPointerUp),this.domElement.removeEventListener("pointercancel",this._onPointerUp),this.domElement.removeEventListener("wheel",this._onMouseWheel),this.domElement.removeEventListener("contextmenu",this._onContextMenu),this.stopListenToKeyEvents(),this.domElement.getRootNode().removeEventListener("keydown",this._interceptControlDown,{capture:!0}),this.domElement.style.touchAction="auto"}dispose(){this.disconnect()}getPolarAngle(){return this._spherical.phi}getAzimuthalAngle(){return this._spherical.theta}getDistance(){return this.object.position.distanceTo(this.target)}listenToKeyEvents(e){e.addEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=e}stopListenToKeyEvents(){this._domElementKeyEvents!==null&&(this._domElementKeyEvents.removeEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=null)}saveState(){this.target0.copy(this.target),this.position0.copy(this.object.position),this.zoom0=this.object.zoom}reset(){this.target.copy(this.target0),this.object.position.copy(this.position0),this.object.zoom=this.zoom0,this.object.updateProjectionMatrix(),this.dispatchEvent(uE),this.update(),this.state=ht.NONE}update(e=null){let t=this.object.position;Ft.copy(t).sub(this.target),Ft.applyQuaternion(this._quat),this._spherical.setFromVector3(Ft),this.autoRotate&&this.state===ht.NONE&&this._rotateLeft(this._getAutoRotationAngle(e)),this.enableDamping?(this._spherical.theta+=this._sphericalDelta.theta*this.dampingFactor,this._spherical.phi+=this._sphericalDelta.phi*this.dampingFactor):(this._spherical.theta+=this._sphericalDelta.theta,this._spherical.phi+=this._sphericalDelta.phi);let i=this.minAzimuthAngle,r=this.maxAzimuthAngle;isFinite(i)&&isFinite(r)&&(i<-Math.PI?i+=dn:i>Math.PI&&(i-=dn),r<-Math.PI?r+=dn:r>Math.PI&&(r-=dn),i<=r?this._spherical.theta=Math.max(i,Math.min(r,this._spherical.theta)):this._spherical.theta=this._spherical.theta>(i+r)/2?Math.max(i,this._spherical.theta):Math.min(r,this._spherical.theta)),this._spherical.phi=Math.max(this.minPolarAngle,Math.min(this.maxPolarAngle,this._spherical.phi)),this._spherical.makeSafe(),this.enableDamping===!0?this.target.addScaledVector(this._panOffset,this.dampingFactor):this.target.add(this._panOffset),this.target.sub(this.cursor),this.target.clampLength(this.minTargetRadius,this.maxTargetRadius),this.target.add(this.cursor);let s=!1;if(this.zoomToCursor&&this._performCursorZoom||this.object.isOrthographicCamera)this._spherical.radius=this._clampDistance(this._spherical.radius);else{let o=this._spherical.radius;this._spherical.radius=this._clampDistance(this._spherical.radius*this._scale),s=o!=this._spherical.radius}if(Ft.setFromSpherical(this._spherical),Ft.applyQuaternion(this._quatInverse),t.copy(this.target).add(Ft),this.object.lookAt(this.target),this.enableDamping===!0?(this._sphericalDelta.theta*=1-this.dampingFactor,this._sphericalDelta.phi*=1-this.dampingFactor,this._panOffset.multiplyScalar(1-this.dampingFactor)):(this._sphericalDelta.set(0,0,0),this._panOffset.set(0,0,0)),this.zoomToCursor&&this._performCursorZoom){let o=null;if(this.object.isPerspectiveCamera){let a=Ft.length();o=this._clampDistance(a*this._scale);let l=a-o;this.object.position.addScaledVector(this._dollyDirection,l),this.object.updateMatrixWorld(),s=!!l}else if(this.object.isOrthographicCamera){let a=new R(this._mouse.x,this._mouse.y,0);a.unproject(this.object);let l=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),this.object.updateProjectionMatrix(),s=l!==this.object.zoom;let c=new R(this._mouse.x,this._mouse.y,0);c.unproject(this.object),this.object.position.sub(c).add(a),this.object.updateMatrixWorld(),o=Ft.length()}else console.warn("WARNING: OrbitControls.js encountered an unknown camera type - zoom to cursor disabled."),this.zoomToCursor=!1;o!==null&&(this.screenSpacePanning?this.target.set(0,0,-1).transformDirection(this.object.matrix).multiplyScalar(o).add(this.object.position):(rd.origin.copy(this.object.position),rd.direction.set(0,0,-1).transformDirection(this.object.matrix),Math.abs(this.object.up.dot(rd.direction))<xL?this.object.lookAt(this.target):(dE.setFromNormalAndCoplanarPoint(this.object.up,this.target),rd.intersectPlane(dE,this.target))))}else if(this.object.isOrthographicCamera){let o=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),o!==this.object.zoom&&(this.object.updateProjectionMatrix(),s=!0)}return this._scale=1,this._performCursorZoom=!1,s||this._lastPosition.distanceToSquared(this.object.position)>Bg||8*(1-this._lastQuaternion.dot(this.object.quaternion))>Bg||this._lastTargetPosition.distanceToSquared(this.target)>Bg?(this.dispatchEvent(uE),this._lastPosition.copy(this.object.position),this._lastQuaternion.copy(this.object.quaternion),this._lastTargetPosition.copy(this.target),!0):!1}_getAutoRotationAngle(e){return e!==null?dn/60*this.autoRotateSpeed*e:dn/60/60*this.autoRotateSpeed}_getZoomScale(e){let t=Math.abs(e*.01);return Math.pow(.95,this.zoomSpeed*t)}_rotateLeft(e){this._sphericalDelta.theta-=e}_rotateUp(e){this._sphericalDelta.phi-=e}_panLeft(e,t){Ft.setFromMatrixColumn(t,0),Ft.multiplyScalar(-e),this._panOffset.add(Ft)}_panUp(e,t){this.screenSpacePanning===!0?Ft.setFromMatrixColumn(t,1):(Ft.setFromMatrixColumn(t,0),Ft.crossVectors(this.object.up,Ft)),Ft.multiplyScalar(e),this._panOffset.add(Ft)}_pan(e,t){let i=this.domElement;if(this.object.isPerspectiveCamera){let r=this.object.position;Ft.copy(r).sub(this.target);let s=Ft.length();s*=Math.tan(this.object.fov/2*Math.PI/180),this._panLeft(2*e*s/i.clientHeight,this.object.matrix),this._panUp(2*t*s/i.clientHeight,this.object.matrix)}else this.object.isOrthographicCamera?(this._panLeft(e*(this.object.right-this.object.left)/this.object.zoom/i.clientWidth,this.object.matrix),this._panUp(t*(this.object.top-this.object.bottom)/this.object.zoom/i.clientHeight,this.object.matrix)):(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - pan disabled."),this.enablePan=!1)}_dollyOut(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale/=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_dollyIn(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale*=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_updateZoomParameters(e,t){if(!this.zoomToCursor)return;this._performCursorZoom=!0;let i=this.domElement.getBoundingClientRect(),r=e-i.left,s=t-i.top,o=i.width,a=i.height;this._mouse.x=r/o*2-1,this._mouse.y=-(s/a)*2+1,this._dollyDirection.set(this._mouse.x,this._mouse.y,1).unproject(this.object).sub(this.object.position).normalize()}_clampDistance(e){return Math.max(this.minDistance,Math.min(this.maxDistance,e))}_handleMouseDownRotate(e){this._rotateStart.set(e.clientX,e.clientY)}_handleMouseDownDolly(e){this._updateZoomParameters(e.clientX,e.clientX),this._dollyStart.set(e.clientX,e.clientY)}_handleMouseDownPan(e){this._panStart.set(e.clientX,e.clientY)}_handleMouseMoveRotate(e){this._rotateEnd.set(e.clientX,e.clientY),this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(dn*this._rotateDelta.x/t.clientHeight),this._rotateUp(dn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd),this.update()}_handleMouseMoveDolly(e){this._dollyEnd.set(e.clientX,e.clientY),this._dollyDelta.subVectors(this._dollyEnd,this._dollyStart),this._dollyDelta.y>0?this._dollyOut(this._getZoomScale(this._dollyDelta.y)):this._dollyDelta.y<0&&this._dollyIn(this._getZoomScale(this._dollyDelta.y)),this._dollyStart.copy(this._dollyEnd),this.update()}_handleMouseMovePan(e){this._panEnd.set(e.clientX,e.clientY),this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd),this.update()}_handleMouseWheel(e){this._updateZoomParameters(e.clientX,e.clientY),e.deltaY<0?this._dollyIn(this._getZoomScale(e.deltaY)):e.deltaY>0&&this._dollyOut(this._getZoomScale(e.deltaY)),this.update()}_handleKeyDown(e){let t=!1;switch(e.code){case this.keys.UP:e.ctrlKey||e.metaKey||e.shiftKey?this._rotateUp(dn*this.rotateSpeed/this.domElement.clientHeight):this._pan(0,this.keyPanSpeed),t=!0;break;case this.keys.BOTTOM:e.ctrlKey||e.metaKey||e.shiftKey?this._rotateUp(-dn*this.rotateSpeed/this.domElement.clientHeight):this._pan(0,-this.keyPanSpeed),t=!0;break;case this.keys.LEFT:e.ctrlKey||e.metaKey||e.shiftKey?this._rotateLeft(dn*this.rotateSpeed/this.domElement.clientHeight):this._pan(this.keyPanSpeed,0),t=!0;break;case this.keys.RIGHT:e.ctrlKey||e.metaKey||e.shiftKey?this._rotateLeft(-dn*this.rotateSpeed/this.domElement.clientHeight):this._pan(-this.keyPanSpeed,0),t=!0;break}t&&(e.preventDefault(),this.update())}_handleTouchStartRotate(e){if(this._pointers.length===1)this._rotateStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._rotateStart.set(i,r)}}_handleTouchStartPan(e){if(this._pointers.length===1)this._panStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panStart.set(i,r)}}_handleTouchStartDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyStart.set(0,s)}_handleTouchStartDollyPan(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enablePan&&this._handleTouchStartPan(e)}_handleTouchStartDollyRotate(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enableRotate&&this._handleTouchStartRotate(e)}_handleTouchMoveRotate(e){if(this._pointers.length==1)this._rotateEnd.set(e.pageX,e.pageY);else{let i=this._getSecondPointerPosition(e),r=.5*(e.pageX+i.x),s=.5*(e.pageY+i.y);this._rotateEnd.set(r,s)}this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(dn*this._rotateDelta.x/t.clientHeight),this._rotateUp(dn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd)}_handleTouchMovePan(e){if(this._pointers.length===1)this._panEnd.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panEnd.set(i,r)}this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd)}_handleTouchMoveDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,s=Math.sqrt(i*i+r*r);this._dollyEnd.set(0,s),this._dollyDelta.set(0,Math.pow(this._dollyEnd.y/this._dollyStart.y,this.zoomSpeed)),this._dollyOut(this._dollyDelta.y),this._dollyStart.copy(this._dollyEnd);let o=(e.pageX+t.x)*.5,a=(e.pageY+t.y)*.5;this._updateZoomParameters(o,a)}_handleTouchMoveDollyPan(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enablePan&&this._handleTouchMovePan(e)}_handleTouchMoveDollyRotate(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enableRotate&&this._handleTouchMoveRotate(e)}_addPointer(e){this._pointers.push(e.pointerId)}_removePointer(e){delete this._pointerPositions[e.pointerId];for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId){this._pointers.splice(t,1);return}}_isTrackingPointer(e){for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId)return!0;return!1}_trackPointer(e){let t=this._pointerPositions[e.pointerId];t===void 0&&(t=new te,this._pointerPositions[e.pointerId]=t),t.set(e.pageX,e.pageY)}_getSecondPointerPosition(e){let t=e.pointerId===this._pointers[0]?this._pointers[1]:this._pointers[0];return this._pointerPositions[t]}_customWheelEvent(e){let t=e.deltaMode,i={clientX:e.clientX,clientY:e.clientY,deltaY:e.deltaY};switch(t){case 1:i.deltaY*=16;break;case 2:i.deltaY*=100;break}return e.ctrlKey&&!this._controlActive&&(i.deltaY*=10),i}};function ML(n){this.enabled!==!1&&(this._pointers.length===0&&(this.domElement.setPointerCapture(n.pointerId),this.domElement.addEventListener("pointermove",this._onPointerMove),this.domElement.addEventListener("pointerup",this._onPointerUp)),!this._isTrackingPointer(n)&&(this._addPointer(n),n.pointerType==="touch"?this._onTouchStart(n):this._onMouseDown(n)))}function EL(n){this.enabled!==!1&&(n.pointerType==="touch"?this._onTouchMove(n):this._onMouseMove(n))}function bL(n){switch(this._removePointer(n),this._pointers.length){case 0:this.domElement.releasePointerCapture(n.pointerId),this.domElement.removeEventListener("pointermove",this._onPointerMove),this.domElement.removeEventListener("pointerup",this._onPointerUp),this.dispatchEvent(fE),this.state=ht.NONE;break;case 1:let e=this._pointers[0],t=this._pointerPositions[e];this._onTouchStart({pointerId:e,pageX:t.x,pageY:t.y});break}}function wL(n){let e;switch(n.button){case 0:e=this.mouseButtons.LEFT;break;case 1:e=this.mouseButtons.MIDDLE;break;case 2:e=this.mouseButtons.RIGHT;break;default:e=-1}switch(e){case es.DOLLY:if(this.enableZoom===!1)return;this._handleMouseDownDolly(n),this.state=ht.DOLLY;break;case es.ROTATE:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=ht.PAN}else{if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=ht.ROTATE}break;case es.PAN:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=ht.ROTATE}else{if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=ht.PAN}break;default:this.state=ht.NONE}this.state!==ht.NONE&&this.dispatchEvent(Hg)}function SL(n){switch(this.state){case ht.ROTATE:if(this.enableRotate===!1)return;this._handleMouseMoveRotate(n);break;case ht.DOLLY:if(this.enableZoom===!1)return;this._handleMouseMoveDolly(n);break;case ht.PAN:if(this.enablePan===!1)return;this._handleMouseMovePan(n);break}}function CL(n){this.enabled===!1||this.enableZoom===!1||this.state!==ht.NONE||(n.preventDefault(),this.dispatchEvent(Hg),this._handleMouseWheel(this._customWheelEvent(n)),this.dispatchEvent(fE))}function TL(n){this.enabled===!1||this.enablePan===!1||this._handleKeyDown(n)}function DL(n){switch(this._trackPointer(n),this._pointers.length){case 1:switch(this.touches.ONE){case ts.ROTATE:if(this.enableRotate===!1)return;this._handleTouchStartRotate(n),this.state=ht.TOUCH_ROTATE;break;case ts.PAN:if(this.enablePan===!1)return;this._handleTouchStartPan(n),this.state=ht.TOUCH_PAN;break;default:this.state=ht.NONE}break;case 2:switch(this.touches.TWO){case ts.DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchStartDollyPan(n),this.state=ht.TOUCH_DOLLY_PAN;break;case ts.DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchStartDollyRotate(n),this.state=ht.TOUCH_DOLLY_ROTATE;break;default:this.state=ht.NONE}break;default:this.state=ht.NONE}this.state!==ht.NONE&&this.dispatchEvent(Hg)}function AL(n){switch(this._trackPointer(n),this.state){case ht.TOUCH_ROTATE:if(this.enableRotate===!1)return;this._handleTouchMoveRotate(n),this.update();break;case ht.TOUCH_PAN:if(this.enablePan===!1)return;this._handleTouchMovePan(n),this.update();break;case ht.TOUCH_DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchMoveDollyPan(n),this.update();break;case ht.TOUCH_DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchMoveDollyRotate(n),this.update();break;default:this.state=ht.NONE}}function IL(n){this.enabled!==!1&&n.preventDefault()}function RL(n){n.key==="Control"&&(this._controlActive=!0,this.domElement.getRootNode().addEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}function NL(n){n.key==="Control"&&(this._controlActive=!1,this.domElement.getRootNode().removeEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}var hE={name:"CopyShader",uniforms:{tDiffuse:{value:null},opacity:{value:1}},vertexShader:`

		varying vec2 vUv;

		void main() {

			vUv = uv;
			gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );

		}`,fragmentShader:`

		uniform float opacity;

		uniform sampler2D tDiffuse;

		varying vec2 vUv;

		void main() {

			vec4 texel = texture2D( tDiffuse, vUv );
			gl_FragColor = opacity * texel;


		}`};var Yn=class{constructor(){this.isPass=!0,this.enabled=!0,this.needsSwap=!0,this.clear=!1,this.renderToScreen=!1}setSize(){}render(){console.error("THREE.Pass: .render() must be implemented in derived pass.")}dispose(){}},PL=new vo(-1,1,1,-1,0,1),zg=class extends zt{constructor(){super(),this.setAttribute("position",new dt([-1,3,0,-1,-1,0,3,-1,0],3)),this.setAttribute("uv",new dt([0,2,0,0,2,0],2))}},OL=new zg,To=class{constructor(e){this._mesh=new wt(OL,e)}dispose(){this._mesh.geometry.dispose()}render(e){e.render(this._mesh,PL)}get material(){return this._mesh.material}set material(e){this._mesh.material=e}};var Do=class extends Yn{constructor(e,t){super(),this.textureID=t!==void 0?t:"tDiffuse",e instanceof Gt?(this.uniforms=e.uniforms,this.material=e):e&&(this.uniforms=Za.clone(e.uniforms),this.material=new Gt({name:e.name!==void 0?e.name:"unspecified",defines:Object.assign({},e.defines),uniforms:this.uniforms,vertexShader:e.vertexShader,fragmentShader:e.fragmentShader})),this.fsQuad=new To(this.material)}render(e,t,i){this.uniforms[this.textureID]&&(this.uniforms[this.textureID].value=i.texture),this.fsQuad.material=this.material,this.renderToScreen?(e.setRenderTarget(null),this.fsQuad.render(e)):(e.setRenderTarget(t),this.clear&&e.clear(e.autoClearColor,e.autoClearDepth,e.autoClearStencil),this.fsQuad.render(e))}dispose(){this.material.dispose(),this.fsQuad.dispose()}};var Ka=class extends Yn{constructor(e,t){super(),this.scene=e,this.camera=t,this.clear=!0,this.needsSwap=!1,this.inverse=!1}render(e,t,i){let r=e.getContext(),s=e.state;s.buffers.color.setMask(!1),s.buffers.depth.setMask(!1),s.buffers.color.setLocked(!0),s.buffers.depth.setLocked(!0);let o,a;this.inverse?(o=0,a=1):(o=1,a=0),s.buffers.stencil.setTest(!0),s.buffers.stencil.setOp(r.REPLACE,r.REPLACE,r.REPLACE),s.buffers.stencil.setFunc(r.ALWAYS,o,4294967295),s.buffers.stencil.setClear(a),s.buffers.stencil.setLocked(!0),e.setRenderTarget(i),this.clear&&e.clear(),e.render(this.scene,this.camera),e.setRenderTarget(t),this.clear&&e.clear(),e.render(this.scene,this.camera),s.buffers.color.setLocked(!1),s.buffers.depth.setLocked(!1),s.buffers.color.setMask(!0),s.buffers.depth.setMask(!0),s.buffers.stencil.setLocked(!1),s.buffers.stencil.setFunc(r.EQUAL,1,4294967295),s.buffers.stencil.setOp(r.KEEP,r.KEEP,r.KEEP),s.buffers.stencil.setLocked(!0)}},od=class extends Yn{constructor(){super(),this.needsSwap=!1}render(e){e.state.buffers.stencil.setLocked(!1),e.state.buffers.stencil.setTest(!1)}};var ad=class{constructor(e,t){if(this.renderer=e,this._pixelRatio=e.getPixelRatio(),t===void 0){let i=e.getSize(new te);this._width=i.width,this._height=i.height,t=new xn(this._width*this._pixelRatio,this._height*this._pixelRatio,{type:ns}),t.texture.name="EffectComposer.rt1"}else this._width=t.width,this._height=t.height;this.renderTarget1=t,this.renderTarget2=t.clone(),this.renderTarget2.texture.name="EffectComposer.rt2",this.writeBuffer=this.renderTarget1,this.readBuffer=this.renderTarget2,this.renderToScreen=!0,this.passes=[],this.copyPass=new Do(hE),this.copyPass.material.blending=hi,this.clock=new wo}swapBuffers(){let e=this.readBuffer;this.readBuffer=this.writeBuffer,this.writeBuffer=e}addPass(e){this.passes.push(e),e.setSize(this._width*this._pixelRatio,this._height*this._pixelRatio)}insertPass(e,t){this.passes.splice(t,0,e),e.setSize(this._width*this._pixelRatio,this._height*this._pixelRatio)}removePass(e){let t=this.passes.indexOf(e);t!==-1&&this.passes.splice(t,1)}isLastEnabledPass(e){for(let t=e+1;t<this.passes.length;t++)if(this.passes[t].enabled)return!1;return!0}render(e){e===void 0&&(e=this.clock.getDelta());let t=this.renderer.getRenderTarget(),i=!1;for(let r=0,s=this.passes.length;r<s;r++){let o=this.passes[r];if(o.enabled!==!1){if(o.renderToScreen=this.renderToScreen&&this.isLastEnabledPass(r),o.render(this.renderer,this.writeBuffer,this.readBuffer,e,i),o.needsSwap){if(i){let a=this.renderer.getContext(),l=this.renderer.state.buffers.stencil;l.setFunc(a.NOTEQUAL,1,4294967295),this.copyPass.render(this.renderer,this.writeBuffer,this.readBuffer,e),l.setFunc(a.EQUAL,1,4294967295)}this.swapBuffers()}Ka!==void 0&&(o instanceof Ka?i=!0:o instanceof od&&(i=!1))}}this.renderer.setRenderTarget(t)}reset(e){if(e===void 0){let t=this.renderer.getSize(new te);this._pixelRatio=this.renderer.getPixelRatio(),this._width=t.width,this._height=t.height,e=this.renderTarget1.clone(),e.setSize(this._width*this._pixelRatio,this._height*this._pixelRatio)}this.renderTarget1.dispose(),this.renderTarget2.dispose(),this.renderTarget1=e,this.renderTarget2=e.clone(),this.writeBuffer=this.renderTarget1,this.readBuffer=this.renderTarget2}setSize(e,t){this._width=e,this._height=t;let i=this._width*this._pixelRatio,r=this._height*this._pixelRatio;this.renderTarget1.setSize(i,r),this.renderTarget2.setSize(i,r);for(let s=0;s<this.passes.length;s++)this.passes[s].setSize(i,r)}setPixelRatio(e){this._pixelRatio=e,this.setSize(this._width,this._height)}dispose(){this.renderTarget1.dispose(),this.renderTarget2.dispose(),this.copyPass.dispose()}};var ld=class extends Yn{constructor(e,t,i=null,r=null,s=null){super(),this.scene=e,this.camera=t,this.overrideMaterial=i,this.clearColor=r,this.clearAlpha=s,this.clear=!0,this.clearDepth=!1,this.needsSwap=!1,this._oldClearColor=new xe}render(e,t,i){let r=e.autoClear;e.autoClear=!1;let s,o;this.overrideMaterial!==null&&(o=this.scene.overrideMaterial,this.scene.overrideMaterial=this.overrideMaterial),this.clearColor!==null&&(e.getClearColor(this._oldClearColor),e.setClearColor(this.clearColor,e.getClearAlpha())),this.clearAlpha!==null&&(s=e.getClearAlpha(),e.setClearAlpha(this.clearAlpha)),this.clearDepth==!0&&e.clearDepth(),e.setRenderTarget(this.renderToScreen?null:i),this.clear===!0&&e.clear(e.autoClearColor,e.autoClearDepth,e.autoClearStencil),e.render(this.scene,this.camera),this.clearColor!==null&&e.setClearColor(this._oldClearColor),this.clearAlpha!==null&&e.setClearAlpha(s),this.overrideMaterial!==null&&(this.scene.overrideMaterial=o),e.autoClear=r}};var pE={name:"OutputShader",uniforms:{tDiffuse:{value:null},toneMappingExposure:{value:1}},vertexShader:`
		precision highp float;

		uniform mat4 modelViewMatrix;
		uniform mat4 projectionMatrix;

		attribute vec3 position;
		attribute vec2 uv;

		varying vec2 vUv;

		void main() {

			vUv = uv;
			gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );

		}`,fragmentShader:`
	
		precision highp float;

		uniform sampler2D tDiffuse;

		#include <tonemapping_pars_fragment>
		#include <colorspace_pars_fragment>

		varying vec2 vUv;

		void main() {

			gl_FragColor = texture2D( tDiffuse, vUv );

			// tone mapping

			#ifdef LINEAR_TONE_MAPPING

				gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );

			#elif defined( REINHARD_TONE_MAPPING )

				gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );

			#elif defined( CINEON_TONE_MAPPING )

				gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );

			#elif defined( ACES_FILMIC_TONE_MAPPING )

				gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );

			#elif defined( AGX_TONE_MAPPING )

				gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );

			#elif defined( NEUTRAL_TONE_MAPPING )

				gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );

			#endif

			// color space

			#ifdef SRGB_TRANSFER

				gl_FragColor = sRGBTransferOETF( gl_FragColor );

			#endif

		}`};var cd=class extends Yn{constructor(){super();let e=pE;this.uniforms=Za.clone(e.uniforms),this.material=new qu({name:e.name,uniforms:this.uniforms,vertexShader:e.vertexShader,fragmentShader:e.fragmentShader}),this.fsQuad=new To(this.material),this._outputColorSpace=null,this._toneMapping=null}render(e,t,i){this.uniforms.tDiffuse.value=i.texture,this.uniforms.toneMappingExposure.value=e.toneMappingExposure,(this._outputColorSpace!==e.outputColorSpace||this._toneMapping!==e.toneMapping)&&(this._outputColorSpace=e.outputColorSpace,this._toneMapping=e.toneMapping,this.material.defines={},Xe.getTransfer(this._outputColorSpace)===ot&&(this.material.defines.SRGB_TRANSFER=""),this._toneMapping===Mg?this.material.defines.LINEAR_TONE_MAPPING="":this._toneMapping===Eg?this.material.defines.REINHARD_TONE_MAPPING="":this._toneMapping===bg?this.material.defines.CINEON_TONE_MAPPING="":this._toneMapping===wg?this.material.defines.ACES_FILMIC_TONE_MAPPING="":this._toneMapping===Sg?this.material.defines.AGX_TONE_MAPPING="":this._toneMapping===Cg&&(this.material.defines.NEUTRAL_TONE_MAPPING=""),this.material.needsUpdate=!0),this.renderToScreen===!0?(e.setRenderTarget(null),this.fsQuad.render(e)):(e.setRenderTarget(t),this.clear&&e.clear(e.autoClearColor,e.autoClearDepth,e.autoClearStencil),this.fsQuad.render(e))}dispose(){this.material.dispose(),this.fsQuad.dispose()}};var Wg=Math.PI/180,jg=14,Ao=1<<jg,ur=256;function ud(){return 110540}function dd(n){return 111320*Math.cos(n*Wg)}function fd(n){return n/ud()}function hd(n,e){return n/dd(e)}function mE(n){return(1-Math.asinh(Math.tan(n))/Math.PI)/2}function pd(n){return(n+180)/360*Ao*ur}function md(n){return mE(n*Wg)*Ao*ur}function $g(n,e){let t=Math.floor((e+180)/360*Ao),i=Math.floor(mE(n*Wg)*Ao);return{x:t,y:i}}function gd(n,e){let t=(n.lon-e.lon)*dd(e.lat),i=(n.lat-e.lat)*ud();return{x:t,z:i}}function gi(n,e){return n.map(t=>gd(t,e))}function is(n){let e=0;for(let t=0;t<n.length;t++){let i=n[t],r=n[(t+1)%n.length];e+=i.x*r.z-r.x*i.z}return e/2}function yd(n){if(n.length<4)return!1;let e=n[0],t=n[n.length-1];return Math.hypot(e.lat-t.lat,e.lon-t.lon)<5e-4}function Ja(n,e){let t=!1;for(let i=0,r=e.length-1;i<e.length;r=i++){let s=e[i],o=e[r];s.z>n.z!=o.z>n.z&&n.x<(o.x-s.x)*(n.z-s.z)/(o.z-s.z||1e-12)+s.x&&(t=!t)}return t}function Qa(n){let e=1/0,t=-1/0,i=1/0,r=-1/0;for(let s of n)s.x<e&&(e=s.x),s.x>t&&(t=s.x),s.z<i&&(i=s.z),s.z>r&&(r=s.z);return{minX:e,maxX:t,minZ:i,maxZ:r}}function rs(n,e,t){if(n.length<4)return n;let i=gi(n,e),r=Gg(i,0,i.length-1,t);return r.sort((o,a)=>o-a),r.map(o=>n[o])}function Gg(n,e,t,i){let r=0,s=0,o=n[e],a=n[t],l=a.x-o.x,c=a.z-o.z,u=Math.hypot(l,c)||1;for(let d=e+1;d<t;d++){let h=n[d],f=Math.max(0,Math.min(1,((h.x-o.x)*l+(h.z-o.z)*c)/(u*u))),m=o.x+f*l-h.x,v=o.z+f*c-h.z,g=Math.hypot(m,v);g>r&&(r=g,s=d)}if(r>i){let d=Gg(n,e,s,i),h=Gg(n,s,t,i);return[...d.slice(0,-1),...h]}return[e,t]}var bn=8e3,fn=bn/2,En=bn/333,ss=0,qg=[{t:0,color:5878620},{t:.1,color:7652448},{t:.22,color:10736485},{t:.36,color:13684082},{t:.5,color:13872490},{t:.66,color:12622434},{t:.8,color:10388317},{t:.9,color:9075812},{t:1,color:14476780}],gE=[15913124,16036227,15703167,15247484,10475723,10995954,14393289,15917723,9356981,15774633,12952537,16242580,14677705,16429472],yE=[3050303,4169034,5221973,5748058,7323486,8833130,8376655],vE=[10194086,9273246,10986165,9407129],_E=qg.map(n=>new xe(n.color)),vd=new R(.38,.85,-.38).normalize(),_d=class{canvas;elevation;overpass;opts;renderer;scene;camera;controls;composer=null;outlineResolution=null;clock=new wo;raf=0;disposed=!1;mapGroup=new fi;waterUniforms={};cloudMesh=null;cloudPuffs=[];marker=null;grid=null;lakes=[];lakeMask=null;treeCount=0;keys=new Set;toonTex;moveTarget=new R;lastCamX=0;lastCamZ=0;buildCount=0;loadToken=0;constructor(e,t,i,r){this.canvas=e,this.elevation=t,this.overpass=i,this.opts=r;let s=e.clientWidth||window.innerWidth,o=e.clientHeight||window.innerHeight,a;try{a=new Ou({canvas:e,antialias:!0})}catch{throw r.onError("Tu navegador no soporta WebGL."),new Error("WebGL no disponible")}this.renderer=a,a.setPixelRatio(Math.min(window.devicePixelRatio||1,2)),a.setSize(s,o),a.setClearColor(12577271,1),a.outputColorSpace=rn,a.toneMapping=pi,this.scene=new Fu,this.scene.fog=new Lu(13167863,55e-6),this.camera=new sn(55,s/o,1,9e4),this.camera.position.set(0,1700,5600),this.controls=new sd(this.camera,e),this.controls.enableDamping=!0,this.controls.dampingFactor=.08,this.controls.enablePan=!1,this.controls.minDistance=6,this.controls.maxDistance=32e3,this.controls.maxPolarAngle=Math.PI/2+.12,this.controls.target.set(0,200,0),this.toonTex=this.makeToonGradient(),this.buildSky(),this.buildSea(),this.scene.add(this.mapGroup),window.addEventListener("keydown",this.onKeyDown),window.addEventListener("keyup",this.onKeyUp),window.addEventListener("blur",this.onBlur),window.addEventListener("resize",this.onResize),this.setupToonOutline(),this.animate()}setupToonOutline(){try{let e=this.canvas.clientWidth||window.innerWidth,t=this.canvas.clientHeight||window.innerHeight,i=this.renderer.getPixelRatio(),r=Math.max(1,Math.round(e*i)),s=Math.max(1,Math.round(t*i)),o=new xn(r,s,{depthBuffer:!0,stencilBuffer:!1});o.depthTexture=new Yr(r,s);let a=new ad(this.renderer,o);a.renderTarget2.depthTexture=new Yr(r,s),a.addPass(new ld(this.scene,this.camera));let l=new te(r,s),c=new Do({uniforms:{tDiffuse:{value:null},tDepth:{value:o.depthTexture},cameraNear:{value:this.camera.near},cameraFar:{value:this.camera.far},resolution:{value:l},outlineColor:{value:new xe(725536)},threshold:{value:.055}},vertexShader:`
          varying vec2 vUv;
          void main() {
            vUv = uv;
            gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
          }
        `,fragmentShader:`
          uniform sampler2D tDiffuse;
          uniform sampler2D tDepth;
          uniform float cameraNear;
          uniform float cameraFar;
          uniform vec2 resolution;
          uniform vec3 outlineColor;
          uniform float threshold;
          varying vec2 vUv;

          float readDepth(vec2 uv) {
            float z = texture2D(tDepth, uv).x;
            float ndc = z * 2.0 - 1.0;
            return (2.0 * cameraNear * cameraFar) / (cameraFar + cameraNear - ndc * (cameraFar - cameraNear));
          }

          void main() {
            vec2 texel = vec2(1.0) / resolution;
            float c = readDepth(vUv);
            float l = readDepth(vUv + vec2(-1.0, 0.0) * texel);
            float r = readDepth(vUv + vec2(1.0, 0.0) * texel);
            float t = readDepth(vUv + vec2(0.0, 1.0) * texel);
            float b = readDepth(vUv + vec2(0.0, -1.0) * texel);
            float e = max(abs(r - l), abs(t - b));
            float thresh = max(c * 0.012, 10.0);
            float line = smoothstep(thresh * 0.7, thresh, e) * threshold * 18.0;
            vec4 color = texture2D(tDiffuse, vUv);
            color.rgb = mix(color.rgb, outlineColor, min(line, 0.9));
            gl_FragColor = color;
          }
        `});a.addPass(c),a.addPass(new cd),this.composer=a,this.outlineResolution=l}catch(e){console.warn("Outline toon no disponible, se usar\xE1 render directo.",e),this.composer=null,this.outlineResolution=null}}load(e){return jt(this,null,function*(){let t=++this.loadToken;this.clearMap(),this.center=e;try{this.opts.onPhase("Descargando elevaci\xF3n (SRTM)..."),this.opts.onProgress(.04);let i=yield this.elevation.getHeightGrid(e,bn,En,l=>{t===this.loadToken&&this.opts.onProgress(.04+.4*l)});if(t!==this.loadToken)return;this.grid=i,this.opts.onPhase("Descargando mapa de OpenStreetMap..."),this.opts.onProgress(.48);let r;try{r=yield this.overpass.fetchArea(e,bn,l=>{t===this.loadToken&&this.opts.onProgress(.48+.32*l)},l=>{t===this.loadToken&&this.opts.onWarning(l)})}catch(l){console.warn("OSM fall\xF3, continuando sin datos vectoriales.",l),r={buildings:[],roads:[],waterPolys:[],vegPolys:[],trees:[],peaks:[]},t===this.loadToken&&this.opts.onWarning("No se pudieron descargar datos de OpenStreetMap. La isla se gener\xF3 solo con terreno y elevaci\xF3n reales.")}if(t!==this.loadToken||(this.opts.onPhase("Construyendo la isla..."),this.opts.onProgress(.84),yield this.buildMap(i,r),t!==this.loadToken))return;this.opts.onPhase("Listo"),this.opts.onProgress(1);let s=Math.max(this.terrainH(i,0,0),ss)+80;this.controls.target.set(0,s,0);let o=s+1100;this.camera.position.set(0,o,5200),this.controls.update(),this.moveTarget.copy(this.controls.target),this.lastCamX=this.controls.target.x,this.lastCamZ=this.controls.target.z;let a=this.computeStats(r);if(t!==this.loadToken)return;this.opts.onReady(a,this.makeMinimap(i))}catch(i){if(t!==this.loadToken||this.disposed)return;this.opts.onError(i instanceof Error?i.message:"Error desconocido al cargar los datos.")}})}dispose(){this.disposed=!0,cancelAnimationFrame(this.raf),window.removeEventListener("keydown",this.onKeyDown),window.removeEventListener("keyup",this.onKeyUp),window.removeEventListener("blur",this.onBlur),window.removeEventListener("resize",this.onResize),this.controls.dispose(),this.toonTex.dispose(),this.composer&&this.composer.dispose(),this.renderer.dispose()}center={lat:0,lon:0};buildMap(e,t){return jt(this,null,function*(){this.lakes=[];for(let i of t.waterPolys){let r=gi(i.coords,this.center),s=is(r);if(Math.abs(s)<500)continue;s<0&&r.reverse();let o=[];if(i.holes)for(let c of i.holes){let u=gi(c,this.center),d=is(u);Math.abs(d)<200||(d>0&&u.reverse(),o.push(u))}let a=Qa(r),l=1/0;for(let c of r){let u=this.terrainH(e,c.x,c.z);u<l&&(l=u)}Number.isFinite(l)||(l=.6),l=Math.max(l,.6),this.lakes.push({local:r,holes:o,level:l,bbox:a})}this.lakes.length>60&&(this.lakes.sort((i,r)=>Math.abs(is(r.local))-Math.abs(is(i.local))),this.lakes=this.lakes.slice(0,60)),this.buildLakeMask(e),this.buildTerrain(e),this.buildBuildings(t),this.buildRoads(t),this.buildVegetation(t),this.buildLakes(),this.buildMarker()})}buildLakeMask(e){if(this.lakeMask=new Float32Array(e.cols*e.rows),this.lakes.length!==0)for(let t of this.lakes){let i=Math.max(0,Math.floor((t.bbox.minX-e.x0)/e.spacingM)),r=Math.min(e.cols-1,Math.floor((t.bbox.maxX-e.x0)/e.spacingM)),s=Math.max(0,Math.floor((t.bbox.minZ-e.z0)/e.spacingM)),o=Math.min(e.rows-1,Math.floor((t.bbox.maxZ-e.z0)/e.spacingM));for(let a=s;a<=o;a++)for(let l=i;l<=r;l++){let c=a*e.cols+l;if(this.lakeMask[c]!==0)continue;let u=e.x0+l*e.spacingM,d=e.z0+a*e.spacingM;if(!Ja({x:u,z:d},t.local))continue;let h=!1;for(let f of t.holes)if(Ja({x:u,z:d},f)){h=!0;break}h||(this.lakeMask[c]=t.level)}}}sampleLakeMask(e,t,i){if(!this.lakeMask)return null;let r=(i-e.z0)/e.spacingM,s=(t-e.x0)/e.spacingM,o=Math.max(0,Math.min(e.rows-2,Math.floor(r))),a=Math.max(0,Math.min(e.cols-2,Math.floor(s))),l=r-o,c=s-a,u=o*e.cols+a,d=this.lakeMask[u],h=this.lakeMask[u+1],f=this.lakeMask[u+e.cols],m=this.lakeMask[u+e.cols+1],v=d+(h-d)*c,g=f+(m-f)*c,p=v+(g-v)*l;return p>.01?p:null}terrainH(e,t,i){let r=this.elevation.heightAt(e,t,i);if(r<=ss)return-1.2;let s=Math.max(Math.abs(t),Math.abs(i))/fn,o=r;if(s>.9){let l=(s-.9)/.1;o=r-(r+1.5)*l*l}let a=this.sampleLakeMask(e,t,i);return a!==null&&o>a&&(o=a-.35),o}buildTerrain(e){let t=e.cols,i=e.rows,r=new Xr(bn,bn,t-1,i-1),s=r.attributes.position,o=Math.max(e.max,220),a=t*i,l=new Float32Array(a),c=new Float32Array(r.attributes.position.count*3),u=new xe,d=new xe;for(let _=0;_<i;_++){let y=-fn+_*En;for(let A=0;A<t;A++){let C=-fn+A*En,S=_*t+A,I;if(e.heights[S]<=ss)I=-1.2;else{I=this.terrainH(e,C,y);let b=this.sampleLakeMask(e,C,y);b!==null&&e.heights[S]>b&&(I=b-.35)}l[S]=I,s.setXYZ(S,C,I,y)}}let h=new Float32Array(a).fill(9999),f=new Int32Array(a),m=0,v=0;for(let _=0;_<a;_++)l[_]<=.1&&(h[_]=0,f[v++]=_);for(;m<v;){let _=f[m++],y=h[_]+1,A=_/t|0,C=_-A*t;if(C>0){let S=_-1;y<h[S]&&(h[S]=y,f[v++]=S)}if(C<t-1){let S=_+1;y<h[S]&&(h[S]=y,f[v++]=S)}if(A>0){let S=_-t;y<h[S]&&(h[S]=y,f[v++]=S)}if(A<i-1){let S=_+t;y<h[S]&&(h[S]=y,f[v++]=S)}}let g=4;for(let _=0;_<i;_++){let y=-fn+_*En;for(let A=0;A<t;A++){let C=-fn+A*En,S=_*t+A,I=l[S],b;if(I<=.1)b=u.setHex(3116996);else{let x=this.sampleLakeMask(e,C,y);if(x!==null&&e.heights[S]>x)b=u.setHex(3116996);else{let D=Math.min(1,Math.max(0,I/o));b=this.rampColor(D,u);let N=h[S];if(N>0&&N<=g){let U=1-N/g;b=b.lerp(d.setHex(15258010),U)}let O=Math.sin(_*127.1+A*311.7)*43758.5453%1;b.offsetHSL(0,0,(Math.abs(O)-.5)*.09)}}c[S*3]=b.r,c[S*3+1]=b.g,c[S*3+2]=b.b}}r.setAttribute("color",new Zt(c,3)),r.computeVertexNormals();let p=new Kt({vertexColors:!0,gradientMap:this.toonTex,side:un}),M=new wt(r,p);M.matrixAutoUpdate=!1,this.mapGroup.add(M)}rampColor(e,t){let i=e*(qg.length-1),r=Math.min(qg.length-2,Math.floor(i)),s=i-r;return t.copy(_E[r]).lerp(_E[r+1],s)}buildBlobShadows(e,t,i){if(e.length===0)return;let r=this.grid,s=[];for(let f of e)this.terrainH(r,f.x,f.z)<0||this.sampleLakeMask(r,f.x,f.z)===null&&s.push(f);if(s.length===0)return;let o=new Gu(1,14),a=new go({color:t,transparent:!0,opacity:i,depthWrite:!1}),l=new Mn(o,a,s.length),c=new tt,u=new R,d=new R,h=new Pt;for(let f=0;f<s.length;f++){let m=s[f],v=this.terrainH(r,m.x,m.z);u.set(m.x,v+.06,m.z),d.set(m.w,m.d,1),h.multiplyQuaternions(BL(m.yaw),LL),c.compose(u,h,d),l.setMatrixAt(f,c)}l.instanceMatrix.needsUpdate=!0,l.renderOrder=2,this.mapGroup.add(l)}buildBuildings(e){let i=e.buildings.slice(0,6e3);if(i.length===0)return;let r=this.grid,s=[];for(let m of i){let v=gi(m.coords,this.center),g=Qa(v),p=(g.minX+g.maxX)/2,M=(g.minZ+g.maxZ)/2;this.terrainH(r,p,M)<0||this.sampleLakeMask(r,p,M)===null&&s.push(m)}if(s.length===0)return;let o=new qr(1,1,1);o.translate(0,.5,0);let a=new Kt({gradientMap:this.toonTex}),l=new Mn(o,a,s.length),c=new tt,u=new Pt,d=new R,h=new R,f=[];for(let m=0;m<s.length;m++){let v=s[m],g=gi(v.coords,this.center),p=Qa(g),M=(p.minX+p.maxX)/2,_=(p.minZ+p.maxZ)/2,y=this.terrainH(r,M,_),A=0,C=-1;for(let ee=0;ee<g.length-1;ee++){let oe=g[ee+1].x-g[ee].x,ge=g[ee+1].z-g[ee].z,we=oe*oe+ge*ge;we>C&&(C=we,A=Math.atan2(ge,oe))}let S=Math.cos(A),I=Math.sin(A),b=1/0,x=-1/0,D=1/0,N=-1/0;for(let ee of g){let oe=ee.x*S+ee.z*I,ge=-ee.x*I+ee.z*S;oe<b&&(b=oe),oe>x&&(x=oe),ge<D&&(D=ge),ge>N&&(N=ge)}let O=Math.min(Math.max(x-b,4),60),U=Math.min(Math.max(N-D,4),60),$=Math.max(2.5,v.height);u.setFromAxisAngle(Io,A),h.set(M,y,_),d.set(O,$,U),c.compose(h,u,d),l.setMatrixAt(m,c);let W=Math.sin(m*12.9898+78.233)*43758.5453,q=W-Math.floor(W),z=Math.floor(q*997)%gE.length,Q=new xe(gE[z]);Q.offsetHSL(0,0,(q-.5)*.05),l.setColorAt(m,Q),f.push({x:M,z:_,w:O*1.15,d:U*1.15,yaw:A})}l.instanceMatrix.needsUpdate=!0,l.instanceColor&&(l.instanceColor.needsUpdate=!0),this.mapGroup.add(l),this.buildBlobShadows(f,662062,.22),this.buildCount=s.length}buildRoads(e){let t=[],i=[],r=0;for(let a of e.roads){if(a.coords.length<2)continue;let l=gi(a.coords,this.center),c=a.width/2;for(let u=0;u<l.length-1;u++){let d=l[u],h=l[u+1],f=h.x-d.x,m=h.z-d.z,v=Math.hypot(f,m);if(v<.05)continue;let g=this.grid,p=this.terrainH(g,d.x,d.z),M=this.terrainH(g,h.x,h.z);if(p<0||M<0||this.sampleLakeMask(g,d.x,d.z)!==null||this.sampleLakeMask(g,h.x,h.z)!==null)continue;let _=(d.x+h.x)/2,y=(d.z+h.z)/2;if(this.terrainH(g,_,y)<0||this.sampleLakeMask(g,_,y)!==null)continue;let A=-m/v,C=f/v,S=p+.25,I=M+.25;t.push(d.x+A*c,S,d.z+C*c),t.push(d.x-A*c,S,d.z-C*c),t.push(h.x+A*c,I,h.z+C*c),t.push(h.x-A*c,I,h.z-C*c),i.push(r,r+2,r+1,r+1,r+2,r+3),r+=4}}if(t.length===0)return;let s=new zt;s.setAttribute("position",new dt(t,3)),s.setIndex(i),s.computeVertexNormals();let o=new Kt({color:14068582,gradientMap:this.toonTex,side:un});this.mapGroup.add(new wt(s,o))}buildVegetation(e){let t=[],r=(x,D,N)=>{if(t.length>=5200||this.terrainH(this.grid,x,D)<=.2||this.sampleLakeMask(this.grid,x,D)!==null)return;let U=(2.6+Math.random()*2.8)*N;t.push({x,z:D,s:U,yaw:Math.random()*Math.PI*2})};for(let x of e.vegPolys){if(t.length>=5200)break;let D=gi(x.coords,this.center),N=is(D);if(Math.abs(N)<200)continue;let O=[];if(x.holes)for(let z of x.holes){let Q=gi(z,this.center);is(Q)!==0&&O.push(Q)}let U=Qa(D),$=x.kind==="forest"?.009:.002,W=Math.min(Math.floor(Math.abs(N)*$),700),q=W*10;for(;W>0&&q-- >0&&t.length<5200;){let z=U.minX+Math.random()*(U.maxX-U.minX),Q=U.minZ+Math.random()*(U.maxZ-U.minZ);if(!Ja({x:z,z:Q},D))continue;let ee=!1;for(let oe of O)if(Ja({x:z,z:Q},oe)){ee=!0;break}ee||(r(z,Q,1),W--)}}for(let x of e.trees){if(t.length>=5200)break;let D=gd(x,this.center);r(D.x,D.z,1)}let s=this.grid,o=26e3;for(;t.length<5200&&o-- >0;){let x=Math.floor(Math.random()*s.cols),D=Math.floor(Math.random()*s.rows),N=-fn+x*En,O=-fn+D*En,U=s.heights[D*s.cols+x];if(U<=ss||U>160||Math.random()>.13)continue;let $=this.terrainH(s,N,O),W=this.terrainH(s,N+En,O),q=this.terrainH(s,N,O+En);Math.abs(W-$)>14||Math.abs(q-$)>14||r(N+(Math.random()-.5)*12,O+(Math.random()-.5)*12,1)}let a=t.length;this.treeCount=a;let l=new tt,c=new Pt,u=new R,d=new R,h=null,f=null,m=null;if(a>0){let x=new Zr(.18,.28,1,5),D=new Mo(1,1,6),N=new Eo(1,1);N.computeBoundingBox();let O=-N.boundingBox.min.y,U=new Kt({color:8016432,gradientMap:this.toonTex}),$=new Kt({gradientMap:this.toonTex}),W=new Array(a),q=0,z=0;for(let we=0;we<a;we++){let G=Math.sin(we*78.233)*43758.5453,J=G-Math.floor(G);W[we]=J<.3,W[we]?z++:q++}h=new Mn(x,U,q),f=new Mn(D,$,q),m=new Mn(N,$,z);let Q=[],ee=0,oe=0,ge=0;for(let we=0;we<a;we++){let G=t[we],J=this.terrainH(this.grid,G.x,G.z),fe=G.s*.5;c.setFromAxisAngle(Io,G.yaw);let re=new xe(yE[Math.floor(Math.random()*yE.length)]);re.offsetHSL((Math.random()-.5)*.02,0,(Math.random()-.5)*.08),W[we]?(u.set(G.x,J+O*G.s*.85,G.z),d.set(G.s*1.1,G.s*.85,G.s*1.1),m.setMatrixAt(oe,l.compose(u,c,d)),m.setColorAt(oe,re),oe++):(u.set(G.x,J+fe/2,G.z),d.set(1,fe,1),h.setMatrixAt(ge,l.compose(u,c,d)),ge++,u.set(G.x,J+fe+.65*G.s,G.z),d.set(G.s,G.s*1.3,G.s),f.setMatrixAt(ee,l.compose(u,c,d)),f.setColorAt(ee,re),ee++),Q.push({x:G.x,z:G.z,w:G.s*1.9,d:G.s*1.9,yaw:0})}h.instanceMatrix.needsUpdate=!0,f.instanceMatrix.needsUpdate=!0,m.instanceMatrix.needsUpdate=!0,f.instanceColor&&(f.instanceColor.needsUpdate=!0),m.instanceColor&&(m.instanceColor.needsUpdate=!0),this.buildBlobShadows(Q,662062,.18)}let v=new ju(1,0);v.computeBoundingBox();let g=-v.boundingBox.min.y,p=new Kt({gradientMap:this.toonTex}),M=[],_=4e3;for(;M.length<130&&_-- >0;){let x=Math.floor(Math.random()*s.cols),D=Math.floor(Math.random()*s.rows),N=-fn+x*En+(Math.random()-.5)*12,O=-fn+D*En+(Math.random()-.5)*12,U=s.heights[D*s.cols+x];U<=ss||U<.55*Math.max(s.max,200)||this.terrainH(s,N,O)<.5||this.sampleLakeMask(s,N,O)===null&&M.push({x:N,z:O,s:1.5+Math.random()*3,yaw:Math.random()*Math.PI})}if(M.length>0){let x=new Mn(v,p,M.length);for(let D=0;D<M.length;D++){let N=M[D],O=this.terrainH(s,N.x,N.z);c.setFromAxisAngle(Io,N.yaw),u.set(N.x,O+g*N.s*.8,N.z),d.set(N.s,N.s*.8,N.s),x.setMatrixAt(D,l.compose(u,c,d)),x.setColorAt(D,new xe(vE[Math.floor(Math.random()*vE.length)]))}x.instanceMatrix.needsUpdate=!0,x.instanceColor&&(x.instanceColor.needsUpdate=!0),this.mapGroup.add(x)}let y=new Zr(.14,.22,1,6),A=new Eo(1,0),C=new Kt({color:9067059,gradientMap:this.toonTex}),S=new Kt({gradientMap:this.toonTex}),I=[],b=5e3;for(;I.length<240&&b-- >0;){let x=Math.floor(Math.random()*s.cols),D=Math.floor(Math.random()*s.rows),N=-fn+x*En+(Math.random()-.5)*14,O=-fn+D*En+(Math.random()-.5)*14,U=s.heights[D*s.cols+x];if(U<=ss||U>26)continue;let $=this.terrainH(s,N,O);$<.6||$>26||this.sampleLakeMask(s,N,O)===null&&I.push({x:N,z:O,s:.8+Math.random()*.7,yaw:Math.random()*Math.PI*2})}if(I.length>0){let x=new Mn(y,C,I.length),D=new Mn(A,S,I.length),N=new xe(4698965);for(let O=0;O<I.length;O++){let U=I[O],$=this.terrainH(s,U.x,U.z),W=U.s*2.4;c.setFromAxisAngle(Io,U.yaw),u.set(U.x,$+W/2,U.z),d.set(1,W,1),x.setMatrixAt(O,l.compose(u,c,d)),u.set(U.x,$+W+.5*U.s,U.z),d.set(U.s*2.6,U.s*.55,U.s*2.6),D.setMatrixAt(O,l.compose(u,c,d));let q=N.clone();q.offsetHSL((Math.random()-.5)*.03,0,(Math.random()-.5)*.06),D.setColorAt(O,q)}x.instanceMatrix.needsUpdate=!0,D.instanceMatrix.needsUpdate=!0,D.instanceColor&&(D.instanceColor.needsUpdate=!0),this.mapGroup.add(x),this.mapGroup.add(D)}h&&this.mapGroup.add(h),f&&this.mapGroup.add(f),m&&this.mapGroup.add(m),this.buildPeaks(e)}buildPeaks(e){if(!e.peaks||e.peaks.length===0)return;let t=new Mo(1,1,4),i=new Kt({color:9402975,gradientMap:this.toonTex}),r=[];for(let u of e.peaks){let d=gd(u,this.center);Math.abs(d.x)>fn||Math.abs(d.z)>fn||this.terrainH(this.grid,d.x,d.z)<1||r.push({x:d.x,z:d.z,h:6+Math.random()*7,yaw:Math.random()*Math.PI})}if(r.length===0)return;let s=new Mn(t,i,r.length),o=new tt,a=new Pt,l=new R,c=new R;for(let u=0;u<r.length;u++){let d=r[u],h=this.terrainH(this.grid,d.x,d.z);a.setFromAxisAngle(Io,d.yaw),l.set(d.x,h+d.h/2,d.z),c.set(2.2,d.h,2.2),s.setMatrixAt(u,o.compose(l,a,c))}s.instanceMatrix.needsUpdate=!0,this.mapGroup.add(s)}buildLakes(){for(let e of this.lakes){let t=new Wa;for(let o=0;o<e.local.length;o++){let a=e.local[o];o===0?t.moveTo(a.x,a.z):t.lineTo(a.x,a.z)}t.closePath();for(let o of e.holes){let a=new xo;for(let l=0;l<o.length;l++){let c=o[l];l===0?a.moveTo(c.x,c.z):a.lineTo(c.x,c.z)}a.closePath(),t.holes.push(a)}let i=new $u(t);i.rotateX(Math.PI/2);let r=new Xu({color:4175848,transparent:!0,opacity:.85,side:un,shininess:40}),s=new wt(i,r);s.position.y=Math.max(e.level,.5),this.mapGroup.add(s)}}buildMarker(){let e=new fi,t=new wt(new Xa(1.7,12,10),new Kt({color:8378699,gradientMap:this.toonTex}));t.scale.set(1,1.15,1);let i=new wt(new Mo(.7,1.7,5),new Kt({color:3112242,gradientMap:this.toonTex}));i.rotation.z=.5,i.position.y=1.9;let r=i.clone();r.rotation.z=-.5,r.position.x=.4;let s=new wt(new Zr(.12,.12,.8,5),new Kt({color:8016432}));s.position.y=1.5,e.add(t,i,r,s);let o=this.terrainH(this.grid,0,0);o<1||(e.position.set(0,o+22,0),this.mapGroup.add(e),this.marker=e)}buildSky(){let e=new Xa(59e3,32,18),t=new wt(e,new Gt({side:Yt,depthWrite:!1,fog:!1,uniforms:{uTop:{value:new xe(1468896)},uMid:{value:new xe(5615346)},uBottom:{value:new xe(13497087)},uSunDir:{value:vd}},vertexShader:`
        varying vec3 vDir;
        void main() {
          vDir = normalize(position);
          gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
        }
      `,fragmentShader:`
        uniform vec3 uTop; uniform vec3 uMid; uniform vec3 uBottom; uniform vec3 uSunDir;
        varying vec3 vDir;
        void main() {
          vec3 d = normalize(vDir);
          float t = clamp(d.y, 0.0, 1.0);
          vec3 col = mix(uBottom, uMid, smoothstep(0.0, 0.3, t));
          col = mix(col, uTop, smoothstep(0.3, 0.78, t));
          float s = dot(d, normalize(uSunDir));
          float core = pow(max(s, 0.0), 2400.0) * 3.2;
          float halo = pow(max(s, 0.0), 42.0) * 0.34;
          col += vec3(1.0, 0.95, 0.82) * (core + halo);
          gl_FragColor = vec4(col, 1.0);
          #include <colorspace_fragment>
        }
      `}));this.scene.add(t),t.renderOrder=0;let i=vd.clone().multiplyScalar(52e3),r=HL(),s=new Ha(new _o({map:r,color:16774857,fog:!1,depthWrite:!1,depthTest:!1}));s.position.copy(i),s.scale.setScalar(7e3),s.renderOrder=3,this.scene.add(s);let o=new Ha(new _o({map:r,color:16771496,transparent:!0,opacity:.35,fog:!1,depthWrite:!1,depthTest:!1}));o.position.copy(i),o.scale.setScalar(16e3),o.renderOrder=2,this.scene.add(o);let a=new Ku(13625599,8364895,.75),l=new Ju(16774102,1.35);l.position.copy(vd).multiplyScalar(4e4),this.scene.add(a,l);let c=new Eo(1,1),u=new Kt({color:16777215,gradientMap:this.toonTex}),d=[],h=Math.floor(26*.4);for(let m=0;m<26;m++){let v=m<h,g=(Math.random()-.5)*(v?22e3:6e4),p=(Math.random()-.5)*(v?16e3:6e4),M=500+Math.random()*(v?900:2600),_=6+Math.random()*14,y=4+Math.floor(Math.random()*4);for(let A=0;A<y;A++){let C=120+Math.random()*260;d.push({base:new R(g+(Math.random()-.5)*900,M+(Math.random()-.5)*130,p+(Math.random()-.5)*700),scale:new R(C,C*.55,C*.8),speed:_,phase:Math.random()*Math.PI*2})}}let f=new Mn(c,u,d.length);f.instanceMatrix.setUsage(ZM),this.cloudMesh=f,this.cloudPuffs=d,this.scene.add(f),f.renderOrder=1}buildSea(){let e={uTime:{value:0},uColorNear:{value:new xe(3125736)},uColorFar:{value:new xe(1802185)},uFogColor:{value:new xe(13167863)},uFogDensity:{value:55e-6},uSunDir:{value:vd}};this.waterUniforms=e;let t=new Xr(9e4,9e4,128,128);t.rotateX(-Math.PI/2);let i=new Gt({uniforms:e,fog:!1,side:un,vertexShader:`
        uniform float uTime;
        varying vec3 vWorldPos;
        void main() {
          vec3 p = position;
          float d = length(p.xz);
          float farAmp = smoothstep(1500.0, 22000.0, d);
          float amp = mix(1.0, 4.5, farAmp);
          p.y += sin(p.x * 0.02 + uTime * 1.6) * amp * 0.35
               + sin(p.z * 0.016 - uTime * 1.2) * amp * 0.35
               + sin((p.x + p.z) * 0.032 + uTime * 2.3) * amp * 0.2;
          vWorldPos = (modelMatrix * vec4(p, 1.0)).xyz;
          gl_Position = projectionMatrix * viewMatrix * vec4(p, 1.0);
        }
      `,fragmentShader:`
        uniform vec3 uColorNear; uniform vec3 uColorFar; uniform vec3 uFogColor;
        uniform float uFogDensity; uniform vec3 uSunDir;
        varying vec3 vWorldPos;
        void main() {
          vec3 viewDir = normalize(cameraPosition - vWorldPos);
          float dist = length(cameraPosition - vWorldPos);
          float fres = pow(1.0 - max(dot(viewDir, vec3(0.0, 1.0, 0.0)), 0.0), 4.0);
          vec3 col = mix(uColorNear, uColorFar, clamp(dist / 22000.0, 0.0, 1.0));
          col = mix(col, vec3(0.85, 0.93, 0.97), fres * 0.5);
          vec3 sunRefl = reflect(normalize(-uSunDir), vec3(0.0, 1.0, 0.0));
          float glint = pow(max(dot(viewDir, sunRefl), 0.0), 140.0);
          col += vec3(1.0, 0.95, 0.7) * glint * 0.85;
          float fog = 1.0 - exp(-uFogDensity * uFogDensity * dist * dist);
          col = mix(col, uFogColor, fog);
          gl_FragColor = vec4(col, 1.0);
          #include <colorspace_fragment>
        }
      `});this.scene.add(new wt(t,i))}makeMinimap(e){let i=document.createElement("canvas");i.width=168,i.height=168;let r=i.getContext("2d");if(!r)return i;let s=r.createImageData(168,168),o=Math.max(e.max,220),a=new xe;for(let l=0;l<168;l++)for(let c=0;c<168;c++){let u=Math.min(e.cols-1,Math.floor(c/168*e.cols)),d=e.rows-1-Math.min(e.rows-1,Math.floor(l/168*e.rows)),h=e.heights[d*e.cols+u],f;h<=ss?f=a.setHex(3116996):f=this.rampColor(Math.min(1,Math.max(0,h/o)),a);let m=(l*168+c)*4;s.data[m]=f.r*255,s.data[m+1]=f.g*255,s.data[m+2]=f.b*255,s.data[m+3]=255}return r.putImageData(s,0,0),i}animate=()=>{if(this.disposed)return;this.raf=requestAnimationFrame(this.animate);let e=Math.min(this.clock.getDelta(),.1),t=this.clock.elapsedTime;if(this.waterUniforms&&(this.waterUniforms.uTime.value=t),this.cloudMesh&&this.cloudPuffs.length>0){for(let i=0;i<this.cloudPuffs.length;i++){let r=this.cloudPuffs[i],s=((r.base.x+r.speed*t)%1e5+1e5)%1e5-5e4;xE.set(s,r.base.y+Math.sin(t*.4+r.phase)*40,r.base.z),ME.compose(xE,FL,r.scale),this.cloudMesh.setMatrixAt(i,ME)}this.cloudMesh.instanceMatrix.needsUpdate=!0}if(this.marker&&(this.marker.position.y=this.terrainH(this.grid,0,0)+22+Math.sin(t*1.8)*1.4,this.marker.rotation.y=t*1.2),this.updateFly(e),this.controls.update(),this.grid){let i=this.camera.position,r=this.terrainH(this.grid,i.x,i.z)+4;i.y<r&&(i.y=r)}this.composer?this.composer.render():this.renderer.render(this.scene,this.camera),this.opts.onFrame(this.controls.target,this.controls.target.y)};updateFly(e){if(!this.grid)return;let t=170,i=this.controls.target,r=0,s=0,o=kL;this.camera.getWorldDirection(o),o.y=0,o.lengthSq()<1e-6&&o.set(0,0,-1),o.normalize();let a=UL;a.set(-o.z,0,o.x),this.keys.has("KeyW")&&(r+=o.x*t*e,s+=o.z*t*e),this.keys.has("KeyS")&&(r-=o.x*t*e,s-=o.z*t*e),this.keys.has("KeyD")&&(r+=a.x*t*e,s+=a.z*t*e),this.keys.has("KeyA")&&(r-=a.x*t*e,s-=a.z*t*e);let l=fn-40;i.x=Math.max(-l,Math.min(l,i.x+r)),i.z=Math.max(-l,Math.min(l,i.z+s));let c=this.keys.has("Space"),u=this.keys.has("KeyC")||this.keys.has("ShiftLeft");c&&(i.y+=t*1.6*e),u&&(i.y-=t*1.6*e);let d=Math.max(this.terrainH(this.grid,i.x,i.z)+28,40),h=c||u?0:Math.min(1,e*7);i.y+=(d-i.y)*h}onKeyDown=e=>{let t=e.target;t&&(t.tagName==="INPUT"||t.tagName==="TEXTAREA"||t.isContentEditable)||(this.keys.add(e.code),["Space","KeyW","KeyA","KeyS","KeyD","KeyC"].includes(e.code)&&e.preventDefault())};onKeyUp=e=>{this.keys.delete(e.code)};onBlur=()=>{this.keys.clear()};onResize=()=>{let e=this.canvas.clientWidth||window.innerWidth,t=this.canvas.clientHeight||window.innerHeight;this.camera.aspect=e/t,this.camera.updateProjectionMatrix(),this.renderer.setSize(e,t),this.composer&&(this.composer.dispose(),this.composer=null,this.outlineResolution=null,this.setupToonOutline())};computeStats(e){let t=0;for(let i of e.roads){let r=gi(i.coords,this.center);for(let s=0;s<r.length-1;s++)t+=Math.hypot(r[s+1].x-r[s].x,r[s+1].z-r[s].z)}return{buildings:this.buildCount,roadKm:t/1e3,trees:this.treeCount,lakes:this.lakes.length,areaKm2:bn*bn/1e6,minElev:this.grid?this.grid.min:0,maxElev:this.grid?this.grid.max:0,center:this.center}}makeToonGradient(){let e=document.createElement("canvas");e.width=64,e.height=4;let t=e.getContext("2d"),i=[[0,"#1a1a1a"],[.45,"#1a1a1a"],[.55,"#6b6b6b"],[.72,"#c9c9c9"],[.78,"#c9c9c9"],[.9,"#ffffff"],[1,"#ffffff"]],r=t.createLinearGradient(0,0,64,0);for(let[o,a]of i)r.addColorStop(o,a);t.fillStyle=r,t.fillRect(0,0,64,4);let s=new za(e);return s.minFilter=vn,s.magFilter=vn,s.generateMipmaps=!1,s}clearMap(){let e=[];this.mapGroup.traverse(i=>{let r=i;r.geometry&&e.push(r.geometry);let s=r.material;s&&e.push(s)}),this.scene.remove(this.mapGroup);let t=new Set;for(let i of e)t.has(i)||(t.add(i),i.dispose&&i.dispose(),i.dispose&&i.dispose());this.mapGroup=new fi,this.scene.add(this.mapGroup),this.marker=null,this.buildCount=0,this.treeCount=0,this.lakes=[],this.lakeMask=null,this.grid=null}},Io=new R(0,1,0),LL=new Pt().setFromAxisAngle(new R(1,0,0),-Math.PI/2),FL=new Pt,xE=new R,ME=new tt,kL=new R,UL=new R,VL=new Pt;function BL(n){return VL.setFromAxisAngle(Io,n)}function HL(){let e=document.createElement("canvas");e.width=64,e.height=64;let t=e.getContext("2d"),i=t.createRadialGradient(64/2,64/2,0,64/2,64/2,64/2);i.addColorStop(0,"rgba(255,255,255,1)"),i.addColorStop(.5,"rgba(255,255,255,0.55)"),i.addColorStop(.72,"rgba(255,255,255,0.12)"),i.addColorStop(1,"rgba(255,255,255,0)"),t.fillStyle=i,t.fillRect(0,0,64,64);let r=new za(e);return r.colorSpace=rn,r}var Xg=["https://overpass-api.de/api/interpreter","https://overpass.kumi.systems/api/interpreter","https://overpass.private.coffee/api/interpreter","https://maps.mail.ru/osm/tools/overpass/api/interpreter","https://overpass.osm.ch/api/interpreter"],EE=Xg,bE={motorway:"motorway",motorway_link:"motorway",trunk:"trunk",trunk_link:"trunk",primary:"primary",primary_link:"primary",secondary:"secondary",secondary_link:"secondary",tertiary:"tertiary",tertiary_link:"tertiary",residential:"residential",unclassified:"residential",living_street:"residential",service:"service",track:"track",path:"path",footway:"path",cycleway:"path",pedestrian:"path",steps:"path",bridleway:"track"},zL={motorway:12,trunk:10,primary:9,secondary:8,tertiary:7,residential:5.5,service:4,track:3.5,path:2},SE=(()=>{class n{selectedMirror=null;setMirror(t){this.selectedMirror=t}fetchArea(t,i,r,s){return jt(this,null,function*(){let o=i/2,a=fd(o),l=hd(o,t.lat),c=t.lat-a,u=t.lon-l,d=t.lat+a,h=t.lon+l,f=`${c.toFixed(6)},${u.toFixed(6)},${d.toFixed(6)},${h.toFixed(6)}`,m=[{label:"edificios",frac:[0,.45],data:`[out:json][timeout:30];way["building"](${f});out geom 2500;`},{label:"carreteras",frac:[.45,.72],data:`[out:json][timeout:30];way["highway"](${f});out geom 3000;`},{label:"agua y vegetaci\xF3n",frac:[.72,.9],data:`[out:json][timeout:30];(way["natural"="water"](${f});way["landuse"="reservoir"](${f});way["water"](${f});way["natural"~"^(wood|scrub|grassland)$"](${f});way["landuse"~"^(forest|grass|meadow)$"](${f});relation["type"="multipolygon"]["natural"="water"](${f});relation["type"="multipolygon"]["landuse"="reservoir"](${f});relation["type"="multipolygon"]["water"](${f});relation["type"="multipolygon"]["natural"~"^(wood|scrub|grassland)$"](${f});relation["type"="multipolygon"]["landuse"~"^(forest|grass|meadow)$"](${f}););(._;way(r););out geom 1500;`},{label:"\xE1rboles y cumbres",frac:[.9,1],data:`[out:json][timeout:30];(node["natural"="tree"](${f});node["natural"="peak"](${f}););out body 9000;`}],v=[],g=0,p=m.reduce((y,A)=>y+A.frac[1]-A.frac[0],0),M=0,_=(y,A)=>jt(this,null,function*(){for(let C=0;C<y.length;C++){let S=y[C];try{let I=yield this.runQuery(S.data,A);v.push(I)}catch(I){g++,v.push([]),console.warn(`Overpass: fall\xF3 la consulta de ${S.label}.`,I),s?.(`No se pudieron descargar ${S.label}. El resto del mapa se gener\xF3 igual.`)}M+=S.frac[1]-S.frac[0],r?.(M/p),C<y.length-1&&(yield this.sleep(350))}});if(yield Promise.all([_([m[0],m[1]],0),_([m[2],m[3]],1)]),g===m.length)throw new Error("Overpass no respondi\xF3 a ninguna consulta");return this.parse(v,t)})}runQuery(t,i=0){return jt(this,null,function*(){let r=this.selectedMirror?[this.selectedMirror]:[...EE.slice(i),...EE.slice(0,i)],s;for(let o of r)for(let a=0;a<2;a++){let l=new AbortController,c=setTimeout(()=>l.abort(),3e4);try{let u=yield fetch(o,{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},body:`data=${encodeURIComponent(t)}`,signal:l.signal});if(clearTimeout(c),u.status===429||u.status===503){s=new Error(`Overpass ${o} rate-limited (${u.status})`),yield this.sleep(1500*(a+1));continue}if(!u.ok){s=new Error(`Overpass ${o} respondi\xF3 ${u.status}`);break}let d=yield u.json();if(d&&typeof d.remark=="string"&&/timeout|error/i.test(d.remark)){s=new Error(`Overpass ${o} fall\xF3: ${d.remark}`),yield this.sleep(600);continue}return d.elements||[]}catch(u){clearTimeout(c),s=u,yield this.sleep(600)}}throw s||new Error("No se pudo contactar con Overpass")})}sleep(t){return new Promise(i=>setTimeout(i,t))}parse(t,i){let r=[],s=[],o=[],a=[],l=[],c=[],u=new Map;for(let p of t)for(let M of p)M.type==="way"&&M.geometry&&M.geometry.length>=2&&u.set(M.id,M.geometry.map(_=>({lat:_.lat,lon:_.lon})));let d=new Set;for(let p of t)for(let M of p){if(M.type!=="relation"||!M.tags||M.tags.type!=="multipolygon"||!M.members)continue;let _=M.tags,y=_.natural==="water"||_.landuse==="reservoir"||!!_.water,A=_.natural==="wood"||_.natural==="scrub"||_.natural==="grassland"||_.landuse==="forest"||_.landuse==="grass"||_.landuse==="meadow"||_.landuse==="village_green";if(!(!y&&!A))for(let C of M.members)C.type==="way"&&C.role!=="inner"&&u.has(C.ref)&&d.add(C.ref)}for(let p of t)for(let M of p){if(M.type==="node"&&M.lat!==void 0&&M.lon!==void 0){M.tags?.natural==="tree"?l.push({lat:M.lat,lon:M.lon}):(M.tags?.natural==="peak"||M.tags?.natural==="volcano")&&c.push({lat:M.lat,lon:M.lon});continue}if(M.type!=="way"||!M.tags||!M.geometry||M.geometry.length<2)continue;let _=M.geometry.map(A=>({lat:A.lat,lon:A.lon})),y=M.tags;if(y.building&&y.building!=="no"&&yd(_)){let A=rs(_,i,4);if(A.length>=4){let C=parseInt(y["building:levels"]||y["building:levels:aboveground"]||"1",10)||1,S=parseFloat(y.height||""),I=Number.isFinite(S)&&S>0?Math.min(S,60):Math.min(Math.max(C*3.2+1.5,4),60);r.push({coords:A,height:I,levels:C})}}else if(y.highway&&bE[y.highway]){let A=bE[y.highway],C=rs(_,i,3);C.length>=2&&s.push({coords:C,cls:A,width:zL[A]})}else if((y.natural==="water"||y.landuse==="reservoir"||y.water)&&yd(_)){if(d.has(M.id))continue;let A=rs(_,i,4);A.length>=4&&o.push({coords:A})}else if((y.natural==="wood"||y.natural==="scrub"||y.natural==="grassland"||y.landuse==="forest"||y.landuse==="grass"||y.landuse==="meadow"||y.landuse==="village_green")&&yd(_)){if(d.has(M.id))continue;let A=rs(_,i,6);if(A.length>=4){let C=y.natural==="wood"||y.landuse==="forest"?"forest":y.natural==="scrub"?"scrub":"grass";a.push({coords:A,kind:C})}}}for(let p of t)for(let M of p){if(M.type!=="relation"||!M.tags||M.tags.type!=="multipolygon"||!M.members)continue;let _=M.tags,y=_.natural==="water"||_.landuse==="reservoir"||!!_.water,A=_.natural==="wood"||_.natural==="scrub"||_.natural==="grassland"||_.landuse==="forest"||_.landuse==="grass"||_.landuse==="meadow"||_.landuse==="village_green";if(!y&&!A)continue;let C=[],S=[];for(let x of M.members){if(x.type!=="way")continue;let D=u.get(x.ref);!D||D.length<2||(x.role==="inner"?S.push(D):C.push(D))}if(C.length===0)continue;let I=wE(C),b=wE(S).filter(x=>x.length>=4);for(let x of I){let D=rs(x,i,6);if(D.length<4)continue;let N=[];for(let O of b){let U=rs(O,i,6);U.length>=4&&N.push(U)}if(y)o.push({coords:D,holes:N.length?N:void 0});else{let O=_.natural==="wood"||_.landuse==="forest"?"forest":_.natural==="scrub"?"scrub":"grass";a.push({coords:D,holes:N.length?N:void 0,kind:O})}}}let h=2500,f=3e3,m=400,v=900,g=8e3;return{buildings:r.length>h?this.sampleArr(r,h):r,roads:s.length>f?this.sampleArr(s,f):s,waterPolys:o.length>m?this.sampleArr(o,m):o,vegPolys:a.length>v?this.sampleArr(a,v):a,trees:l.length>g?this.sampleArr(l,g):l,peaks:c}}sampleArr(t,i){let r=t.length/i,s=[];for(let o=0;o<i;o++)s.push(t[Math.min(t.length-1,Math.floor(o*r))]);return s}static \u0275fac=function(i){return new(i||n)};static \u0275prov=mt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function wE(n){let e=[],t=new Array(n.length).fill(!1),i=(r,s)=>{let o=r.lat-s.lat,a=r.lon-s.lon;return o*o+a*a<1e-8};for(;;){let r=-1;for(let a=0;a<n.length;a++)if(!t[a]){r=a;break}if(r<0)break;let s=[...n[r]];t[r]=!0;let o=!0;for(;o;){o=!1;for(let a=0;a<n.length;a++){if(t[a])continue;let l=n[a],c=s[0],u=s[s.length-1],d=l[0],h=l[l.length-1];if(i(d,u)){s.push(...l.slice(1)),t[a]=!0,o=!0;break}if(i(h,u)){s.push(...l.slice(0,-1).reverse()),t[a]=!0,o=!0;break}if(i(h,c)){s.unshift(...l.slice(0,-1)),t[a]=!0,o=!0;break}if(i(d,c)){s.unshift(...l.slice(1).reverse()),t[a]=!0,o=!0;break}}}s.length>=3&&i(s[0],s[s.length-1])&&e.push(s)}return e}var WL="https://s3.amazonaws.com/elevation-tiles-prod/terrarium",CE=(()=>{class n{cache=new Map;getHeightGrid(t,i,r,s){let o=`${t.lat.toFixed(5)},${t.lon.toFixed(5)},${i},${r}`,a=this.cache.get(o);if(a)return a;let l=this.fetchGrid(t,i,r,s).catch(c=>{throw this.cache.delete(o),c});return this.cache.set(o,l),l}heightAt(t,i,r){let s=(r-t.z0)/t.spacingM,o=(i-t.x0)/t.spacingM,a=Math.max(0,Math.min(t.rows-2,Math.floor(s))),l=Math.max(0,Math.min(t.cols-2,Math.floor(o))),c=s-a,u=o-l,d=a*t.cols+l,h=t.heights[d],f=t.heights[d+1],m=t.heights[d+t.cols],v=t.heights[d+t.cols+1],g=h+(f-h)*u,p=m+(v-m)*u;return g+(p-g)*c}fetchGrid(t,i,r,s){return jt(this,null,function*(){let o=i/2,a=fd(o),l=hd(o,t.lat),c=t.lat+a,u=t.lat-a,d=t.lon+l,h=t.lon-l,f=pd(h),m=pd(d),v=md(c),g=md(u),p=Math.floor(f)-1,M=Math.ceil(m)+1,_=Math.floor(v)-1,y=Math.ceil(g)+1,A=M-p+1,C=y-_+1,S=$g(c,h),I=$g(u,d),b=Ao-1,x=Math.max(0,Math.min(S.x,I.x)),D=Math.min(b,Math.max(S.x,I.x)),N=Math.max(0,Math.min(S.y,I.y)),O=Math.min(b,Math.max(S.y,I.y)),U=[];for(let ge=N;ge<=O;ge++)for(let we=x;we<=D;we++)U.push([we,ge]);let $=new Float32Array(A*C).fill(NaN),W=0;yield Promise.allSettled(U.map(G=>jt(this,[G],function*([ge,we]){try{let J=yield this.fetchTile(ge,we),fe=ge*ur-p,re=we*ur-_;for(let be=0;be<ur;be++){let Te=re+be;if(Te<0||Te>=C)continue;let Ne=be*ur,nt=Te*A;for(let ze=0;ze<ur;ze++){let _t=fe+ze;_t<0||_t>=A||($[nt+_t]=J[Ne+ze])}}}catch(J){console.warn(`Tile de elevaci\xF3n ${ge}/${we} no disponible, usando hueco.`,J)}W++,s?.(W/U.length)})));let q=Math.floor(i/r)+1,z=q,Q=new Float32Array(q*z),ee=1/0,oe=-1/0;for(let ge=0;ge<z;ge++){let we=-o+ge*r,G=t.lat+we/ud(),J=md(G);for(let fe=0;fe<q;fe++){let re=-o+fe*r,be=t.lon+re/dd(t.lat),Te=pd(be),Ne=this.sample($,A,C,Te-p,J-_),nt=Number.isFinite(Ne)?Ne:0;Q[ge*q+fe]=nt,nt<ee&&(ee=nt),nt>oe&&(oe=nt)}}return{cols:q,rows:z,spacingM:r,x0:-o,z0:-o,heights:Q,min:ee,max:oe}})}fetchTile(t,i){return jt(this,null,function*(){let r=`${WL}/${jg}/${t}/${i}.png`,s=new AbortController,o=setTimeout(()=>s.abort(),3e4);try{let a=yield fetch(r,{signal:s.signal});if(!a.ok)throw new Error(`No se pudo descargar el tile de elevaci\xF3n ${t}/${i} (${a.status})`);let l=yield a.blob(),c=document.createElement("canvas"),u=null;if(typeof createImageBitmap=="function")try{u=yield createImageBitmap(l)}catch{u=null}if(u)c.width=u.width,c.height=u.height;else{let g=URL.createObjectURL(l);try{let p=yield jL(g);c.width=p.naturalWidth,c.height=p.naturalHeight,u=p}finally{URL.revokeObjectURL(g)}}let d=c.getContext("2d",{willReadFrequently:!0});if(!d)throw new Error("Canvas 2D no disponible");d.drawImage(u,0,0);let h=c.width,f=c.height,m=d.getImageData(0,0,h,f).data,v=new Float32Array(h*f);for(let g=0;g<h*f;g++){let p=g*4;v[g]=m[p]*256+m[p+1]+m[p+2]/256-32768}return u&&"close"in u&&u.close(),v}finally{clearTimeout(o)}})}sample(t,i,r,s,o){let a=Math.max(0,Math.min(i-1,s)),l=Math.max(0,Math.min(r-1,o)),c=Math.floor(a),u=Math.floor(l),d=Math.min(i-1,c+1),h=Math.min(r-1,u+1),f=a-c,m=l-u,v=u*i+c,g=u*i+d,p=h*i+c,M=h*i+d,_=t[v],y=t[g],A=t[p],C=t[M];if(Number.isFinite(_)&&Number.isFinite(y)&&Number.isFinite(A)&&Number.isFinite(C)){let S=_+(y-_)*f,I=A+(C-A)*f;return S+(I-S)*m}for(let S=-1;S<=1;S++)for(let I=-1;I<=1;I++){let b=Math.max(0,Math.min(r-1,u+S)),x=Math.max(0,Math.min(i-1,c+I)),D=t[b*i+x];if(Number.isFinite(D))return D}return NaN}static \u0275fac=function(i){return new(i||n)};static \u0275prov=mt({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function jL(n){return new Promise((e,t)=>{let i=new Image;i.onload=()=>e(i),i.onerror=()=>t(new Error("No se pudo decodificar la imagen")),i.src=n})}var qL=["sceneCanvas"],XL=["minimap"];function YL(n,e){if(n&1&&(je(0,"option",18),qe(1),He()),n&2){let t=e.$implicit;Di("value",t.value),Dt(),Ki(t.label)}}function ZL(n,e){if(n&1){let t=Mc();je(0,"button",19),mn("click",function(){let r=si(t).$implicit,s=Ir();return oi(s.usePreset(r))}),Ai(1,"span",20),qe(2),He()}if(n&2){let t=e.$implicit;xc("--pc",t.color),Dt(2),Rr("",t.name," ")}}function KL(n,e){if(n&1&&(je(0,"div",21)(1,"div")(2,"span"),qe(3,"Edificios"),He(),je(4,"b"),qe(5),Nr(6,"number"),He()(),je(7,"div")(8,"span"),qe(9,"Carreteras"),He(),je(10,"b"),qe(11),Nr(12,"number"),He()(),je(13,"div")(14,"span"),qe(15,"Lagos"),He(),je(16,"b"),qe(17),He()(),je(18,"div")(19,"span"),qe(20,"Elevaci\xF3n"),He(),je(21,"b"),qe(22),Nr(23,"number"),Nr(24,"number"),He()(),je(25,"div")(26,"span"),qe(27,"\xC1rea"),He(),je(28,"b"),qe(29),Nr(30,"number"),He()()()),n&2){let t=Ir();Dt(5),Ki(k0(6,6,t.stats.buildings)),Dt(6),Rr("",Os(12,8,t.stats.roadKm,"1.1-1")," km"),Dt(6),Ki(t.stats.lakes),Dt(5),$h("",Os(23,11,t.stats.minElev,"1.0-0")," \u2013 ",Os(24,14,t.stats.maxElev,"1.0-0")," m"),Dt(7),Rr("",Os(30,17,t.stats.areaKm2,"1.0-0")," km\xB2")}}function JL(n,e){if(n&1&&(je(0,"div",22),qe(1),He()),n&2){let t=Ir();Dt(),Ki(t.warning)}}function QL(n,e){if(n&1&&(je(0,"div",23)(1,"div",24),Ai(2,"div",25),je(3,"h2"),qe(4),He(),je(5,"div",26),Ai(6,"div",27),He(),je(7,"p",28),qe(8),Nr(9,"number"),He()()()),n&2){let t=Ir();Dt(4),Ki(t.phase),Dt(2),xc("width",t.progress*100,"%"),Dt(2),Rr("",Os(9,4,t.progress*100,"1.0-0"),"%")}}function eF(n,e){if(n&1){let t=Mc();je(0,"div",23)(1,"div",29)(2,"h2"),qe(3,"\xA1Ooga booga! Algo sali\xF3 mal"),He(),je(4,"p"),qe(5),He(),je(6,"button",10),mn("click",function(){si(t);let r=Ir();return oi(r.explore())}),qe(7,"Volver a intentar"),He()()()}if(n&2){let t=Ir();Dt(5),Ki(t.errorMsg)}}var TE=(()=>{class n{zone;elevation;overpass;canvasRef;minimapRef;presets=[{name:"Bali \xB7 Ubud",color:"#2ecc71",lat:-8.5069,lon:115.2625},{name:"Santorini",color:"#3498db",lat:36.4167,lon:25.4324},{name:"Medell\xEDn",color:"#e67e22",lat:6.2447,lon:-75.573},{name:"San Francisco",color:"#e74c3c",lat:37.7749,lon:-122.4194}];lat=-8.5069;lon=115.2625;state="idle";phase="";progress=0;errorMsg="";warning="";stats=null;mirrors=[{value:"",label:"Auto (fallback por orden)"},...Xg.map(t=>({value:t,label:t.replace("https://","").replace("/api/interpreter","")}))];selectedMirror="";scene=null;minimapBase=null;minimapCtx=null;minimapScale=1;prevDot={x:-1,y:-1};minimapHasBase=!1;constructor(t,i,r){this.zone=t,this.elevation=i,this.overpass=r}ngAfterViewInit(){let t=this.canvasRef.nativeElement;this.zone.runOutsideAngular(()=>{try{this.scene=new _d(t,this.elevation,this.overpass,{onPhase:i=>this.zone.run(()=>this.phase=i),onProgress:i=>this.zone.run(()=>this.progress=i),onReady:(i,r)=>this.zone.run(()=>{this.state="ready",this.stats=i,this.minimapBase=r,this.drawMinimapBase()}),onError:i=>this.zone.run(()=>{this.state="error",this.errorMsg=i}),onWarning:i=>this.zone.run(()=>{this.warning=i}),onFrame:i=>this.drawMinimapDot(i.x,i.z)})}catch{this.zone.run(()=>{this.state="error",this.errorMsg="No se pudo iniciar WebGL en este navegador."})}}),this.minimapCtx=this.minimapRef.nativeElement.getContext("2d"),this.minimapRef.nativeElement.width=168,this.minimapRef.nativeElement.height=168,this.explore()}explore(){if(!this.scene)return;if(!Number.isFinite(this.lat)||!Number.isFinite(this.lon)){this.state="error",this.errorMsg="Ingresa coordenadas num\xE9ricas v\xE1lidas.";return}let t=Math.max(-85,Math.min(85,this.lat)),i=Math.max(-180,Math.min(180,this.lon));this.state="loading",this.progress=0,this.stats=null,this.errorMsg="",this.warning="",this.minimapHasBase=!1;let r=this.minimapCtx;r&&r.clearRect(0,0,this.minimapRef.nativeElement.width,this.minimapRef.nativeElement.height),this.zone.runOutsideAngular(()=>this.scene.load({lat:t,lon:i}))}usePreset(t){this.lat=t.lat,this.lon=t.lon,this.explore()}onMirrorChange(t){this.overpass.setMirror(t||null)}drawMinimapBase(){let t=this.minimapCtx,i=this.minimapBase;if(!t||!i)return;let r=this.minimapRef.nativeElement.width;t.clearRect(0,0,r,r),t.drawImage(i,0,0,r,r),this.minimapScale=r/(i.width||r),this.prevDot={x:-1,y:-1},this.minimapHasBase=!0}drawMinimapDot(t,i){let r=this.minimapCtx;if(!r||!this.minimapBase||!this.minimapHasBase)return;let o=this.minimapRef.nativeElement.width,a=(t+bn/2)/bn*o,l=(bn/2-i)/bn*o,c=this.prevDot;if(!(c.x>=0&&Math.abs(c.x-a)<1&&Math.abs(c.y-l)<1)){if(c.x>=0){let u=this.minimapScale,d=7;r.drawImage(this.minimapBase,c.x/u,c.y/u,d*2/u,d*2/u,c.x-d,c.y-d,d*2,d*2)}r.beginPath(),r.arc(a,l,4,0,Math.PI*2),r.fillStyle="#ffd32a",r.fill(),r.lineWidth=1.5,r.strokeStyle="#a06a1c",r.stroke(),c.x=a,c.y=l}}ngOnDestroy(){this.scene?.dispose()}static \u0275fac=function(i){return new(i||n)(Je(Ht),Je(CE),Je(SE))};static \u0275cmp=_c({type:n,selectors:[["app-map-view"]],viewQuery:function(i,r){if(i&1&&(Gh(qL,7),Gh(XL,7)),i&2){let s;Wh(s=jh())&&(r.canvasRef=s.first),Wh(s=jh())&&(r.minimapRef=s.first)}},decls:46,vars:9,consts:[["sceneCanvas",""],["minimap",""],[1,"scene-canvas"],[1,"panel"],[1,"title"],[1,"subtitle"],[1,"field"],["type","number","step","0.0001",3,"ngModelChange","keydown.enter","ngModel"],[1,"mirror",3,"ngModelChange","ngModel"],[3,"value",4,"ngFor","ngForOf"],[1,"btn-explore",3,"click"],[1,"presets"],["class","btn-preset",3,"--pc","click",4,"ngFor","ngForOf"],["class","stats",4,"ngIf"],[1,"minimap"],[1,"hint"],["class","toast",4,"ngIf"],["class","overlay",4,"ngIf"],[3,"value"],[1,"btn-preset",3,"click"],[1,"dot"],[1,"stats"],[1,"toast"],[1,"overlay"],[1,"load-card"],[1,"spinner"],[1,"bar"],[1,"fill"],[1,"pct"],[1,"load-card","error"]],template:function(i,r){if(i&1){let s=Mc();Ai(0,"canvas",2,0),je(2,"aside",3)(3,"h1",4),qe(4,"Isla "),je(5,"span"),qe(6,"Wumpa"),He(),qe(7," 3D"),He(),je(8,"p",5),qe(9,"8 km navegables \xB7 OSM + SRTM reales"),He(),je(10,"label",6)(11,"span"),qe(12,"Latitud"),He(),je(13,"input",7),oa("ngModelChange",function(a){return si(s),Ec(r.lat,a)||(r.lat=a),oi(a)}),mn("keydown.enter",function(){return si(s),oi(r.explore())}),He()(),je(14,"label",6)(15,"span"),qe(16,"Longitud"),He(),je(17,"input",7),oa("ngModelChange",function(a){return si(s),Ec(r.lon,a)||(r.lon=a),oi(a)}),mn("keydown.enter",function(){return si(s),oi(r.explore())}),He()(),je(18,"label",6)(19,"span"),qe(20,"Servidor Overpass"),He(),je(21,"select",8),oa("ngModelChange",function(a){return si(s),Ec(r.selectedMirror,a)||(r.selectedMirror=a),oi(a)}),mn("ngModelChange",function(a){return si(s),oi(r.onMirrorChange(a))}),Rs(22,YL,2,2,"option",9),He()(),je(23,"button",10),mn("click",function(){return si(s),oi(r.explore())}),qe(24,"Explorar"),He(),je(25,"div",11),Rs(26,ZL,3,3,"button",12),He(),Rs(27,KL,31,20,"div",13),He(),Ai(28,"canvas",14,1),je(30,"div",15)(31,"b"),qe(32,"W A S D"),He(),qe(33," volar \xA0\xB7\xA0 "),je(34,"b"),qe(35,"arrastrar"),He(),qe(36," orbitar \xA0\xB7\xA0 "),je(37,"b"),qe(38,"rueda"),He(),qe(39," zoom \xA0\xB7\xA0 "),je(40,"b"),qe(41,"Espacio / C"),He(),qe(42,` subir y bajar
`),He(),Rs(43,JL,2,1,"div",16)(44,QL,10,7,"div",17)(45,eF,8,1,"div",17)}i&2&&(Dt(13),sa("ngModel",r.lat),Dt(4),sa("ngModel",r.lon),Dt(4),sa("ngModel",r.selectedMirror),Dt(),Di("ngForOf",r.mirrors),Dt(4),Di("ngForOf",r.presets),Dt(),Di("ngIf",r.stats),Dt(16),Di("ngIf",r.warning),Dt(),Di("ngIf",r.state==="loading"),Dt(),Di("ngIf",r.state==="error"))},dependencies:[Ac,Dc,Qh,ep,Sx,Ex,wx,Bc,yp,zc,vx,gp],styles:["[_nghost-%COMP%]{display:block;width:100%;height:100%;position:relative}.scene-canvas[_ngcontent-%COMP%]{position:absolute;inset:0;width:100%;height:100%;cursor:grab}.panel[_ngcontent-%COMP%]{position:absolute;top:16px;right:16px;width:264px;max-height:calc(100vh - 32px);overflow-y:auto;background:linear-gradient(180deg,#ffedc2,#ffd98a);border:4px solid #6a4a2a;border-radius:18px;box-shadow:0 6px #3c260e59,0 14px 30px #00000040;padding:14px 16px 16px;color:#4a2f18;z-index:10}.title[_ngcontent-%COMP%]{margin:0;font-size:26px;line-height:1.05;letter-spacing:.5px;color:#8a3a1f;text-shadow:1px 1px 0 #ffd98a}.title[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{color:#2e8b3f;text-shadow:1px 1px 0 #c8f0a0}.subtitle[_ngcontent-%COMP%]{margin:2px 0 12px;font-size:12px;opacity:.85}.field[_ngcontent-%COMP%]{display:block;margin-bottom:8px}.field[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{display:block;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;margin-bottom:2px}.field[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]{width:100%;padding:7px 9px;font-size:14px;font-family:inherit;border:3px solid #6a4a2a;border-radius:10px;background:#fffdf5;color:#4a2f18;outline:none}.field[_ngcontent-%COMP%]   input[_ngcontent-%COMP%]:focus{border-color:#e67e22}.field[_ngcontent-%COMP%]   select[_ngcontent-%COMP%]{width:100%;padding:7px 9px;font-size:13px;font-family:inherit;border:3px solid #6a4a2a;border-radius:10px;background:#fffdf5;color:#4a2f18;outline:none;cursor:pointer}.field[_ngcontent-%COMP%]   select[_ngcontent-%COMP%]:focus{border-color:#e67e22}.btn-explore[_ngcontent-%COMP%]{width:100%;margin:6px 0 12px;padding:11px 0;font-size:17px;font-weight:700;font-family:inherit;letter-spacing:1px;color:#fff;background:linear-gradient(180deg,#ffb347,#f0762c);border:3px solid #6a3a14;border-radius:12px;box-shadow:0 4px #8f4a16;cursor:pointer;transition:transform .08s ease,box-shadow .08s ease}.btn-explore[_ngcontent-%COMP%]:active{transform:translateY(3px);box-shadow:0 1px #8f4a16}.presets[_ngcontent-%COMP%]{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:12px}.btn-preset[_ngcontent-%COMP%]{display:flex;align-items:center;gap:6px;padding:7px 6px;font-size:12px;font-weight:700;font-family:inherit;color:#4a2f18;background:#fffdf5;border:3px solid #6a4a2a;border-radius:10px;cursor:pointer;transition:transform .08s ease}.btn-preset[_ngcontent-%COMP%]:hover{transform:translateY(-1px);background:#fff}.btn-preset[_ngcontent-%COMP%]   .dot[_ngcontent-%COMP%]{width:10px;height:10px;border-radius:50%;background:var(--pc, #ccc);border:2px solid #6a4a2a;flex:0 0 10px}.stats[_ngcontent-%COMP%]{border-top:3px dashed #b98a4a;padding-top:10px;display:grid;gap:4px}.stats[_ngcontent-%COMP%]   div[_ngcontent-%COMP%]{display:flex;justify-content:space-between;font-size:12px}.stats[_ngcontent-%COMP%]   b[_ngcontent-%COMP%]{color:#8a3a1f}.minimap[_ngcontent-%COMP%]{position:absolute;left:16px;bottom:16px;width:168px;height:168px;border:4px solid #6a4a2a;border-radius:14px;box-shadow:0 4px #3c260e59,0 10px 22px #00000040;background:#2f8fc4;z-index:10}.hint[_ngcontent-%COMP%]{position:absolute;left:50%;bottom:16px;transform:translate(-50%);padding:8px 14px;background:#fffdf5eb;border:3px solid #6a4a2a;border-radius:12px;font-size:12px;color:#4a2f18;white-space:nowrap;z-index:10}.hint[_ngcontent-%COMP%]   b[_ngcontent-%COMP%]{color:#e67e22}.toast[_ngcontent-%COMP%]{position:absolute;top:16px;left:50%;transform:translate(-50%);max-width:70%;padding:10px 16px;background:#fffdf5f2;border:3px solid #6a4a2a;border-radius:12px;font-size:13px;color:#b33737;z-index:20}.overlay[_ngcontent-%COMP%]{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;background:#12488259;z-index:30}.load-card[_ngcontent-%COMP%]{width:320px;padding:22px 24px 26px;text-align:center;background:linear-gradient(180deg,#ffedc2,#ffd98a);border:4px solid #6a4a2a;border-radius:18px;box-shadow:0 8px #3c260e59,0 18px 40px #0000004d;color:#4a2f18}.load-card[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{margin:12px 0 14px;font-size:18px;min-height:24px}.load-card[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{font-size:13px;margin:8px 0 0}.pct[_ngcontent-%COMP%]{font-weight:700;color:#8a3a1f}.bar[_ngcontent-%COMP%]{height:16px;background:#6a4a2a;border-radius:9px;overflow:hidden;border:2px solid #3d2a14}.fill[_ngcontent-%COMP%]{height:100%;background:linear-gradient(90deg,#7fd14f,#2ecc71);transition:width .15s linear}.spinner[_ngcontent-%COMP%]{width:46px;height:46px;margin:4px auto 0;border-radius:50%;border:6px solid #b98a4a;border-top-color:#2e8b3f;animation:_ngcontent-%COMP%_spin .9s linear infinite}@keyframes _ngcontent-%COMP%_spin{to{transform:rotate(360deg)}}.error[_ngcontent-%COMP%]   h2[_ngcontent-%COMP%]{color:#b33737}"]})}return n})();var DE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=_c({type:n,selectors:[["app-root"]],decls:1,vars:0,template:function(i,r){i&1&&Ai(0,"app-map-view")},dependencies:[TE],encapsulation:2})}return n})();up(DE).catch(n=>console.error(n));
